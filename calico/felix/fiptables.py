# -*- coding: utf-8 -*-
# Copyright 2015 Metaswitch Networks
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
felix.fiptables
~~~~~~~~~~~~

IP tables management functions.
"""
from collections import defaultdict
import copy
import logging
import random
import time
import itertools
import re

from gevent import subprocess
import gevent

from calico.felix import frules, futils
from calico.felix.actor import (
    Actor, actor_message, ResultOrExc, SplitBatchAndRetry
)
from calico.felix.frules import FELIX_PREFIX
from calico.felix.futils import FailedSystemCall


_log = logging.getLogger(__name__)

_correlators = ("ipt-%s" % ii for ii in itertools.count())
MAX_IPT_RETRIES = 10
MAX_IPT_BACKOFF = 0.2


class IptablesUpdater(Actor):
    """
    Actor that owns and applies updates to a particular iptables table.
    Supports batching updates for performance and dependency tracking
    between chains.

    iptables safety
    ~~~~~~~~~~~~~~~

    Concurrent access to the same table is not allowed by the
    underlying iptables architecture so there should be one instance of
    this class for each table.  Each IP version has its own set of
    non-conflicting tables.

    However, this class tries to be robust against concurrent access
    from outside the process by detecting and retrying such errors.

    Batching support
    ~~~~~~~~~~~~~~~~

    This actor supports batching of multiple updates. It applies updates that
    are on the queue in one atomic batch. This is dramatically faster than
    issuing single iptables requests.

    If a request fails, it does a binary chop using the SplitBatchAndRetry
    mechanism to report the error to the correct request.

    Dependency tracking
    ~~~~~~~~~~~~~~~~~~~

    To offload a lot of coordination complexity from the classes that
    use this one, this class supports tracking dependencies between chains
    and programming stubs for missing chains:

    * When calling rewrite_chains() the caller must supply a dict that
      maps from chain to a set of chains it requires (i.e. the chains
      that appear in its --jump and --goto targets).

    * Any chains that are required but not present are created as "stub"
      chains, which drop all traffic. They are marked as such in the
      iptables rules with an iptables comment.

    * When a required chain is later explicitly created, the stub chain is
      replaced with the required contents of the chain.

    * If a required chain is explicitly deleted, it is rewritten as a stub
      chain.

    * If a chain exists only as a stub chain to satisfy a dependency, then it
      is cleaned up when the dependency is removed.

    """

    def __init__(self, table, ip_version=4):
        super(IptablesUpdater, self).__init__(qualifier="v%d" % ip_version)
        self._table = table
        if ip_version == 4:
            self._restore_cmd = "iptables-restore"
            self._save_cmd = "iptables-save"
            self._iptables_cmd = "iptables"
        else:
            assert ip_version == 6
            self._restore_cmd = "ip6tables-restore"
            self._save_cmd = "ip6tables-save"
            self._iptables_cmd = "ip6tables"

        self._chains_in_dataplane = None
        """
        Set of chains that we know are actually in the dataplane.  Loaded
        at start of day and then kept in sync.
        """
        self._grace_period_finished = False
        """
        Flag that is set after the graceful restart window is over.
        """

        self._explicitly_prog_chains = set()
        """Set of chains that we've explicitly programmed."""

        self._required_chains = defaultdict(set)
        """Map from chain name to the set of names of chains that it
        depends on."""
        self._requiring_chains = defaultdict(set)
        """Map from chain to the set of chains that depend on it.
        Inverse of self.required_chains."""

        # Since it's fairly complex to keep track of the changes required
        # for a particular batch and still be able to roll-back the changes
        # to our data structures, we delegate to a per-batch object that
        # does that calculation.
        self._txn = None
        """:type _Transaction: object used to track index changes
        for this batch."""
        self._completion_callbacks = None
        """List of callbacks to issue once the current batch completes."""

        # Avoid duplicating init logic.
        self._refresh_chains_in_dataplane()
        self._reset_batched_work()

    def _reset_batched_work(self):
        """Reset the per-batch state in preparation for a new batch."""
        self._txn = _Transaction(self._explicitly_prog_chains,
                                         self._required_chains,
                                         self._requiring_chains)
        self._completion_callbacks = []

    def _refresh_chains_in_dataplane(self):
        raw_ipt_output = subprocess.check_output([self._save_cmd, "--table",
                                                  self._table])
        self._chains_in_dataplane = _extract_our_chains(self._table,
                                                        raw_ipt_output)

    def _read_unreferenced_chains(self):
        """
        Read the list of chains in the dataplane which are not referenced.

        :returns list[str]: list of chains currently in the dataplane that
            are not referenced by other chains.
        """
        raw_ipt_output = subprocess.check_output(
            [self._iptables_cmd, "--wait", "--list", "--table", self._table])
        return _extract_our_unreffed_chains(raw_ipt_output)

    @actor_message()
    def rewrite_chains(self, update_calls_by_chain,
                       dependent_chains, callback=None):
        """
        Atomically apply a set of updates to the table.

        :param update_calls_by_chain: map from chain name to list of
               iptables-style update calls,
               e.g. {"chain_name": ["-A chain_name -j ACCEPT"]}. Chain will
               be flushed.
        :param dependent_chains: map from chain name to a set of chains
               that that chain requires to exist. They will be created
               (with a default drop) if they don't exist.
        :returns CalledProcessError if a problem occurred.
        """
        # We actually apply the changes in _finish_msg_batch().  Index the
        # changes by table and chain.
        _log.info("Iptables update: %s", update_calls_by_chain)
        _log.info("Iptables deps: %s", dependent_chains)
        for chain, updates in update_calls_by_chain.iteritems():
            # TODO: double-check whether this flush is needed.
            updates = ["--flush %s" % chain] + updates
            deps = dependent_chains.get(chain, set())
            self._txn.store_rewrite_chain(chain, updates, deps)
        if callback:
            self._completion_callbacks.append(callback)

    # Does direct table manipulation, forbid batching with other messages.
    @actor_message(needs_own_batch=True)
    def ensure_rule_inserted(self, rule_fragment):
        """
        Runs the given rule fragment, prefixed with --insert. If the
        rule was already present, it is removed and reinserted at the
        start of the chain.

        This covers the case where we need to insert a rule into the
        pre-existing kernel chains (only). For chains that are owned by Felix,
        use the more robust approach of rewriting the whole chain using
        rewrite_chains().

        :param rule_fragment: fragment to be inserted. For example,
           "INPUT --jump felix-INPUT"
        """
        try:
            # Do an atomic delete + insert of the rule.  If the rule already
            # exists then the rule will be moved to the start of the chain.
            _log.info("Attempting to move any existing instance of rule %r"
                      "to top of chain.", rule_fragment)
            self._execute_iptables(['*%s' % self._table,
                                    '--delete %s' % rule_fragment,
                                    '--insert %s' % rule_fragment,
                                    'COMMIT'],
                                   fail_log_level=logging.DEBUG)
        except FailedSystemCall:
            # Assume the rule didn't exist. Try inserting it.
            _log.info("Didn't find any existing instance of rule %r, "
                      "inserting it instead.")
            self._execute_iptables(['*%s' % self._table,
                                    '--insert %s' % rule_fragment,
                                    'COMMIT'])

    @actor_message()
    def delete_chains(self, chain_names, callback=None):
        # We actually apply the changes in _finish_msg_batch().  Index the
        # changes by table and chain.
        _log.info("Deleting chains %s", chain_names)
        for chain in chain_names:
            self._txn.store_delete(chain)
        if callback:
            self._completion_callbacks.append(callback)

    # It's much simpler to do cleanup in its own batch so that it doesn't have
    # to worry about in-flight updates.
    @actor_message(needs_own_batch=True)
    def cleanup(self):
        """
        Tries to clean up any left-over chains from a previous run that
        are no longer required.
        """
        _log.info("Cleaning up left-over iptables state.")

        # Start with the current state.
        self._refresh_chains_in_dataplane()

        required_chains = set(self._requiring_chains.keys())
        if not self._grace_period_finished:
            # Ensure that all chains that are required but not explicitly
            # programmed are stubs.
            #
            # We have to do this at the end of the graceful restart period
            # during which we may have re-used old chains.
            chains_to_stub = (required_chains -
                              self._explicitly_prog_chains)
            _log.info("Graceful restart window finished, stubbing out "
                      "chains: %s", chains_to_stub)
            try:
                self._stub_out_chains(chains_to_stub)
            except NothingToDo:
                pass
            self._grace_period_finished = True

        # Now the generic cleanup, look for chains that we're not expecting to
        # be there and delete them.
        chains_we_tried_to_delete = set()
        finished = False
        while not finished:
            # Try to delete all the unreferenced chains, we use a loop to
            # ensure that we then clean up any chains that become unreferenced
            # when we delete the previous lot.
            unreferenced_chains = self._read_unreferenced_chains()
            orphans = (unreferenced_chains -
                       self._explicitly_prog_chains -
                       required_chains)
            if not chains_we_tried_to_delete.issuperset(orphans):
                _log.info("Cleanup found these unreferenced chains to "
                          "delete: %s", orphans)
                chains_we_tried_to_delete.update(orphans)
                self._delete_best_effort(orphans)
            else:
                # We've already tried to delete all the chains we found,
                # give up.
                _log.info("Cleanup finished, deleted %d chains, failed to "
                          "delete these chains: %s",
                          len(chains_we_tried_to_delete) - len(orphans),
                          orphans)
                finished = True

        # Then some sanity checks:
        temp_chains = self._chains_in_dataplane
        self._refresh_chains_in_dataplane()
        if temp_chains != self._chains_in_dataplane:
            # We want to know about this but it's not fatal.
            _log.error("Chains in data plane inconsistent with calculated "
                       "index.  In dataplane but not in index: %s; In index: "
                       "but not dataplane: %s.",
                       self._chains_in_dataplane - temp_chains,
                       temp_chains - self._chains_in_dataplane)

        missing_chains = ((self._explicitly_prog_chains | required_chains) -
                          self._chains_in_dataplane)
        if missing_chains:
            # This is fatal, some of our chains have disappeared.
            _log.error("Some of our chains disappeared from the dataplane: %s."
                       " Raising an exception.",
                       missing_chains)
            raise IptablesInconsistent(
                "Felix chains missing from iptables: %s" % missing_chains)

    def _start_msg_batch(self, batch):
        self._reset_batched_work()
        return batch

    def _finish_msg_batch(self, batch, results):
        start = time.time()
        try:
            # We use two passes to update the dataplane.  In the first pass,
            # we make any updates, create new chains and replace to-be-deleted
            # chains with stubs (in case we fail to delete them below).
            try:
                input_lines = self._calculate_ipt_modify_input()
            except NothingToDo:
                _log.info("%s no updates in this batch.", self)
            else:
                self._execute_iptables(input_lines)
                _log.info("%s Successfully processed iptables updates.", self)
                self._chains_in_dataplane.update(self._txn.affected_chains)
        except (IOError, OSError, FailedSystemCall) as e:
            if isinstance(e, FailedSystemCall):
                rc = e.retcode
            else:
                rc = "unknown"
            if len(batch) == 1:
                # We only executed a single message, report the failure.
                _log.error("Non-retryable %s failure. RC=%s",
                           self._restore_cmd, rc)
                if self._completion_callbacks:
                    self._completion_callbacks[0](e)
                final_result = ResultOrExc(None, e)
                results[0] = final_result
            else:
                _log.error("Non-retryable error from a combined batch, "
                           "splitting the batch to narrow down culprit.")
                raise SplitBatchAndRetry()
        else:
            # Modify succeeded, update our indexes for next time.
            self._update_indexes()
            # Make a best effort to delete the chains we no longer want.
            # If we fail due to a stray reference from an orphan chain, we
            # should catch them on the next cleanup().
            self._delete_best_effort(self._txn.chains_to_delete)
            for c in self._completion_callbacks:
                c(None)
        finally:
            self._reset_batched_work()

        end = time.time()
        _log.debug("Batch time: %.2f %s", end - start, len(batch))

    def _delete_best_effort(self, chains):
        """
        Try to delete all the chains in the input list. Any errors are silently
        swallowed.
        """
        if not chains:
            return
        chain_batches = [list(chains)]
        while chain_batches:
            batch = chain_batches.pop(0)
            try:
                # Try the next batch of chains...
                _log.debug("Attempting to delete chains: %s", batch)
                self._attempt_delete(batch)
            except (IOError, OSError, FailedSystemCall):
                _log.warning("Deleting chains %s failed", batch)
                if len(batch) > 1:
                    # We were trying to delete multiple chains, split the
                    # batch in half and put the batches back on the queue to
                    # try again.
                    _log.info("Batch was of length %s, splitting", len(batch))
                    split_point = len(batch) // 2
                    first_half = batch[:split_point]
                    second_half = batch[split_point:]
                    assert len(first_half) + len(second_half) == len(batch)
                    if chain_batches:
                        chain_batches[0][:0] = second_half
                    else:
                        chain_batches[:0] = [second_half]
                    chain_batches[:0] = [first_half]
                else:
                    # Only trying to delete one chain, give up.  It must still
                    # be referenced.
                    _log.error("Failed to delete chain %s, giving up. Maybe "
                               "it is still referenced?", batch[0])
            else:
                _log.debug("Deleted chains %s successfully, remaining "
                           "batches: %s", batch, len(chain_batches))

    def _stub_out_chains(self, chains):
        input_lines = self._calculate_ipt_stub_input(chains)
        self._execute_iptables(input_lines)

    def _attempt_delete(self, chains):
        try:
            input_lines = self._calculate_ipt_delete_input(chains)
        except NothingToDo:
            _log.debug("No chains to delete %s", chains)
        else:
            self._execute_iptables(input_lines, fail_log_level=logging.WARNING)
            self._chains_in_dataplane -= chains

    def _update_indexes(self):
        """
        Called after successfully processing a batch, updates the
        indices with the values calculated by the _Transaction.
        """
        self._explicitly_prog_chains = self._txn.expl_prog_chains
        self._required_chains = self._txn.required_chns
        self._requiring_chains = self._txn.requiring_chns

    def _calculate_ipt_modify_input(self):
        """
        Calculate the input for phase 1 of a batch, where we only modify and
        create chains.

        :raises NothingToDo: if the batch requires no modify operations.
        """
        # Valid input looks like this.
        #
        # *table
        # :chain_name
        # :chain_name_2
        # -F chain_name
        # -A chain_name -j ACCEPT
        # COMMIT
        #
        # The chains are created if they don't exist.
        input_lines = []
        affected_chains = self._txn.affected_chains
        for chain in affected_chains:
            if (self._grace_period_finished or
                    chain not in self._chains_in_dataplane or
                    chain not in self._txn.chains_to_stub_out):
                # We're going to rewrite or delete this chain below, mark it
                # for creation/flush.
                input_lines.append(":%s -" % chain)
        for chain in self._txn.chains_to_stub_out:
            if (self._grace_period_finished or
                    chain not in self._chains_in_dataplane):
                # After graceful restart completes, we stub out all chains;
                # during the graceful restart, we reuse any existing chains
                # that happen to be there.
                input_lines.extend(_stub_drop_rules(chain))
        for chain in self._txn.chains_to_delete:
            # Explicitly told to delete this chain.  Rather than delete it
            # outright, we stub it out first.  Then, if the delete fails
            # due to the chain still being referenced, at least the chain is
            # "safe".  Stubbing it out also stops it from referencing other
            # chains, accidentally keeping them alive.
            input_lines.extend(_stub_drop_rules(chain))
        for chain, chain_updates in self._txn.updates.iteritems():
            input_lines.extend(chain_updates)
        if not input_lines:
            raise NothingToDo
        return ["*%s" % self._table] + input_lines + ["COMMIT"]

    def _calculate_ipt_delete_input(self, chains):
        """
        Calculate the input for phase 2 of a batch, where we actually
        try to delete chains.

        :raises NothingToDo: if the batch requires no delete operations.
        """
        input_lines = []
        found_delete = False
        input_lines.append("*%s" % self._table)
        for chain_name in chains:
            # Delete the chain
            input_lines.append(":%s -" % chain_name)
            input_lines.append("--delete-chain %s" % chain_name)
            found_delete = True
        input_lines.append("COMMIT")
        if found_delete:
            return input_lines
        else:
            raise NothingToDo()

    def _calculate_ipt_stub_input(self, chains):
        """
        Calculate input to replace the given chains with stubs.
        """
        input_lines = []
        found_chain_to_stub = False
        input_lines.append("*%s" % self._table)
        for chain_name in chains:
            # Stub the chain
            input_lines.append(":%s -" % chain_name)
            input_lines.extend(_stub_drop_rules(chain_name))
            found_chain_to_stub = True
        input_lines.append("COMMIT")
        if found_chain_to_stub:
            return input_lines
        else:
            raise NothingToDo()

    def _execute_iptables(self, input_lines, fail_log_level=logging.ERROR):
        """
        Runs ip(6)tables-restore with the given input.  Retries iff
        the COMMIT fails.

        :raises FailedSystemCall: if the command fails on a non-commit
            line or if it repeatedly fails and retries are exhausted.
        """
        backoff = 0.01
        num_tries = 0
        success = False
        while not success:
            input_str = "\n".join(input_lines) + "\n"
            _log.debug("%s input:\n%s", self._restore_cmd, input_str)

            # Run iptables-restore in noflush mode so that it doesn't
            # blow away all the tables we're not touching.
            cmd = [self._restore_cmd, "--noflush", "--verbose"]
            try:
                futils.check_call(cmd, input_str=input_str)
            except FailedSystemCall as e:
                # Parse the output to determine if error is retryable.
                retryable, detail = _parse_ipt_restore_error(input_lines,
                                                             e.stderr)
                num_tries += 1
                if retryable:
                    if num_tries < MAX_IPT_RETRIES:
                        _log.info("%s failed with retryable error. Retry in "
                                  "%.2fs", self._iptables_cmd, backoff)
                        gevent.sleep(backoff)
                        if backoff > MAX_IPT_BACKOFF:
                            backoff = MAX_IPT_BACKOFF
                        backoff *= (1.5 + random.random())
                        continue
                    else:
                        _log.log(
                            fail_log_level,
                            "Failed to run %s.  Out of retries: %s.\n"
                            "Output:\n%s\n"
                            "Error:\n%s\n"
                            "Input was:\n%s",
                            self._restore_cmd, detail, e.stdout, e.stderr,
                            input_str)
                else:
                    _log.log(
                        fail_log_level,
                        "%s failed with non-retryable error: %s.\n"
                        "Output:\n%s\n"
                        "Error:\n%s\n"
                        "Input was:\n%s",
                        self._restore_cmd, detail, e.stdout, e.stderr,
                        input_str)
                raise
            else:
                success = True


class _Transaction(object):
    """
    This class keeps track of a sequence of updates to an
    IptablesUpdater's indexing data structures.

    It takes a copy of the data structures at creation and then
    gets fed the sequence of updates and deletes; then, on-demand
    it calculates the dataplane deltas that are required and
    caches the results.

    The general idea is that, if the iptables-restore call fails,
    the Transaction object can be thrown away, leaving the
    IptablesUpdater's state unchanged.

    """
    def __init__(self,
                 old_expl_prog_chains,
                 old_deps,
                 old_requiring_chains):
        # Figure out what stub chains should already be present.
        self.already_stubbed = (set(old_requiring_chains.keys()) -
                                old_expl_prog_chains)

        # Deltas.
        self.updates = {}
        self._deletes = set()

        # New state.  These will be copied back to the IptablesUpdater
        # if the transaction succeeds.
        self.expl_prog_chains = copy.deepcopy(old_expl_prog_chains)
        self.required_chns = copy.deepcopy(old_deps)
        self.requiring_chns = copy.deepcopy(old_requiring_chains)

        # Memoized values of the properties below.  See chains_to_stub(),
        # affected_chains() and chains_to_delete() below.
        self._chains_to_stub = None
        self._affected_chains = None
        self._chains_to_delete = None

    def store_delete(self, chain):
        """
        Records the delete of the given chain, updating the per-batch
        indexes as required.
        """
        _log.debug("Storing delete of chain %s", chain)
        assert chain is not None
        # Clean up dependency index.
        self._update_deps(chain, set())
        # Mark for deletion.
        self._deletes.add(chain)
        # Remove any now-stale rewrite state.
        self.updates.pop(chain, None)
        self.expl_prog_chains.discard(chain)
        self._invalidate_cache()

    def store_rewrite_chain(self, chain, updates, dependencies):
        """
        Records the rewrite of the given chain, updating the per-batch
        indexes as required.
        """
        _log.debug("Storing updates to chain %s", chain)
        assert chain is not None
        assert updates is not None
        assert dependencies is not None
        # Clean up reverse dependency index.
        self._update_deps(chain, dependencies)
        # Remove any deletion, if present.
        self._deletes.discard(chain)
        # Store off the update.
        self.updates[chain] = updates
        self.expl_prog_chains.add(chain)
        self._invalidate_cache()

    def _update_deps(self, chain, new_deps):
        """
        Updates the forward/backward dependency indexes for the given
        chain.
        """
        # Remove all the old deps from the reverse index..
        old_deps = self.required_chns.get(chain, set())
        for dependency in old_deps:
            self.requiring_chns[dependency].discard(chain)
            if not self.requiring_chns[dependency]:
                del self.requiring_chns[dependency]
        # Add in the new deps to the reverse index.
        for dependency in new_deps:
            self.requiring_chns[dependency].add(chain)
        # And store them off in the forward index.
        if new_deps:
            self.required_chns[chain] = new_deps
        else:
            self.required_chns.pop(chain, None)

    def _invalidate_cache(self):
        self._chains_to_stub = None
        self._affected_chains = None
        self._chains_to_delete = None

    @property
    def affected_chains(self):
        """
        The set of chains that are touched by this update (whether
        deleted, modified, or to be stubbed).
        """
        if self._affected_chains is None:
            updates = set(self.updates.keys())
            stubs = self.chains_to_stub_out
            deletes = self.chains_to_delete
            _log.debug("Affected chains: deletes=%s, updates=%s, stubs=%s",
                       deletes, updates, stubs)
            self._affected_chains = deletes | updates | stubs
        return self._affected_chains

    @property
    def chains_to_stub_out(self):
        """
        The set of chains that need to be stubbed as part of this update.
        """
        if self._chains_to_stub is None:
            # Don't stub out chains that we're now explicitly programming.
            impl_required_chains = (self.referenced_chains -
                                    self.expl_prog_chains)
            # Don't stub out chains that are already stubbed.
            self._chains_to_stub = impl_required_chains - self.already_stubbed
        return self._chains_to_stub

    @property
    def chains_to_delete(self):
        """
        The set of chains to actually delete from the dataplane.  Does
        not include the chains that we need to stub out.
        """
        if self._chains_to_delete is None:
            # We'd like to get rid of these chains if we can.
            chains_we_dont_want = self._deletes | self.already_stubbed
            _log.debug("Chains we'd like to delete: %s", chains_we_dont_want)
            # But we need to keep the chains that are explicitly programmed
            # or referenced.
            chains_we_need = self.expl_prog_chains | self.referenced_chains
            _log.debug("Chains we still need for some reason: %s",
                       chains_we_need)
            self._chains_to_delete = chains_we_dont_want - chains_we_need
            _log.debug("Chains we can delete: %s", self._chains_to_delete)
        return self._chains_to_delete

    @property
    def referenced_chains(self):
        """
        Set of chains referred to by other chains.

        Does not include chains that are explicitly programmed but not
        referenced by anything else.
        """
        return set(self.requiring_chns.keys())


def _stub_drop_rules(chain):
    """
    :return: List of rule fragments to replace the given chain with a
        single drop rule.
    """
    return ["--flush %s" % chain,
            frules.commented_drop_fragment(chain,
                                           'WARNING Missing chain DROP:')]


def _extract_our_chains(table, raw_ipt_save_output):
    """
    Parses the output from iptables-save to extract the set of
    felix-programmed chains.
    """
    chains = set()
    current_table = None
    for line in raw_ipt_save_output.splitlines():
        line = line.strip()
        if line.startswith("*"):
            current_table = line[1:]
        elif line.startswith(":") and current_table == table:
            chain = line[1:line.index(" ")]
            if chain.startswith(FELIX_PREFIX):
                chains.add(chain)
    return chains


def _extract_our_unreffed_chains(raw_ipt_output):
    """
    Parses the output from "ip(6)tables --list" to find the set of
    felix-programmed chains that are not referenced.
    """
    chains = set()
    last_line = None
    for line in raw_ipt_output.splitlines():
        # Look for lines that look like this after a blank line.
        # Chain ufw-user-output (1 references)
        if ((not last_line or not last_line.strip()) and
                line.startswith("Chain")):
            if "policy" in line:
                _log.debug("Skipping root-level chain")
                continue
            m = re.match(r'^Chain ([^ ]+) \((\d+).+\)', line)
            assert m, "Regex failed to match Chain line %r" % line
            chain_name = m.group(1)
            ref_count = int(m.group(2))
            _log.debug("Found chain %s, ref count %s", chain_name, ref_count)
            if chain_name.startswith(FELIX_PREFIX) and ref_count == 0:
                chains.add(chain_name)
        last_line = line
    return chains


def _parse_ipt_restore_error(input_lines, err):
    """
    Parses the stderr output from an iptables-restore call.

    :param input_lines: list of lines of input that we passed to
        iptables-restore.  (Used for debugging.)
    :param str err: captures stderr from iptables-restore.
    :return tuple[bool,str]: tuple, the first (bool) element indicates
        whether the error is retryable; the second is a detail message.
    """
    match = re.search(r"line (\d+) failed", err)
    if match:
        # Have a line number, work out if this was a commit
        # failure, which is caused by concurrent access and is
        # retryable.
        line_number = int(match.group(1))
        _log.debug("ip(6)tables-restore failure on line %s", line_number)
        line_index = line_number - 1
        offending_line = input_lines[line_index]
        if offending_line.strip == "COMMIT":
            return True, "COMMIT failed; likely concurrent access."
        else:
            return False, "Line %s failed: %s" % (line_number, offending_line)
    else:
        return False, "ip(6)tables-restore failed with output: %s" % err



class NothingToDo(Exception):
    pass


class IptablesInconsistent(Exception):
    pass
