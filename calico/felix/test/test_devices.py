# -*- coding: utf-8 -*-
# Copyright 2014, 2015 Metaswitch Networks
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
felix.test.test_devices
~~~~~~~~~~~

Test the device handling code.
"""
import logging
import mock
import os
import sys
import uuid
from contextlib import nested

if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest

import calico.felix.devices as devices
import calico.felix.futils as futils
import calico.felix.test.stub_utils as stub_utils

# Logger
log = logging.getLogger(__name__)

# Canned mock calls representing clean entry to/exit from a context manager.
M_ENTER = mock.call().__enter__()
M_CLEAN_EXIT = mock.call().__exit__(None, None, None)


class TestDevices(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_interface_exists(self):
        tap = "tap" + str(uuid.uuid4())[:11]

        with mock.patch('os.path.exists', return_value=True):
            self.assertTrue(devices.interface_exists(tap))
            os.path.exists.assert_called_with("/sys/class/net/" + tap)

        with mock.patch('os.path.exists', return_value=False):
            self.assertFalse(devices.interface_exists(tap))
            os.path.exists.assert_called_with("/sys/class/net/" + tap)

    def test_add_route(self):
        tap = "tap" + str(uuid.uuid4())[:11]
        mac = stub_utils.get_mac()
        retcode = futils.CommandOutput("", "")

        type = futils.IPV4
        ip = "1.2.3.4"
        with mock.patch('calico.felix.futils.check_call', return_value=retcode):
            devices.add_route(type, ip, tap, mac)
            futils.check_call.assert_any_call(['arp', '-s', ip, mac, '-i', tap])
            futils.check_call.assert_called_with(["ip", "route", "replace", ip, "dev", tap])

        type = futils.IPV6
        ip = "2001::"
        with mock.patch('calico.felix.futils.check_call', return_value=retcode):
            devices.add_route(type, ip, tap, mac)
            futils.check_call.assert_called_with(["ip", "-6", "route", "replace", ip, "dev", tap])

    def test_del_route(self):
        tap = "tap" + str(uuid.uuid4())[:11]
        retcode = futils.CommandOutput("", "")

        type = futils.IPV4
        ip = "1.2.3.4"
        with mock.patch('calico.felix.futils.check_call', return_value=retcode):
            devices.del_route(type, ip, tap)
            futils.check_call.assert_any_call(['arp', '-d', ip, '-i', tap])
            futils.check_call.assert_called_with(["ip", "route", "del", ip, "dev", tap])

        type = futils.IPV6
        ip = "2001::"
        with mock.patch('calico.felix.futils.check_call', return_value=retcode):
            devices.del_route(type, ip, tap)
            futils.check_call.assert_called_once_with(["ip", "-6", "route", "del", ip, "dev", tap])


    def test_list_interface_ips(self):
        type = futils.IPV4
        tap = "tap" + str(uuid.uuid4())[:11]

        retcode = futils.CommandOutput("", "")
        with mock.patch('calico.felix.futils.check_call', return_value=retcode):
            ips = devices.list_interface_ips(type, tap)
            futils.check_call.assert_called_once_with(["ip", "route", "list", "dev", tap])
            self.assertFalse(ips)

        stdout = "10.11.9.90  scope link"
        retcode = futils.CommandOutput(stdout, "")
        with mock.patch('calico.felix.futils.check_call', return_value=retcode):
            ips = devices.list_interface_ips(type, tap)
            futils.check_call.assert_called_once_with(["ip", "route", "list", "dev", tap])
            self.assertEqual(ips, set(["10.11.9.90"]))

        stdout = "10.11.9.90  scope link\nblah-di-blah not valid\nx\n"
        retcode = futils.CommandOutput(stdout, "")
        with mock.patch('calico.felix.futils.check_call', return_value=retcode):
            ips = devices.list_interface_ips(type, tap)
            futils.check_call.assert_called_once_with(["ip", "route", "list", "dev", tap])
            self.assertEqual(ips, set(["10.11.9.90"]))

        type = futils.IPV6
        stdout = "2001:: scope link\n"
        retcode = futils.CommandOutput(stdout, "")
        with mock.patch('calico.felix.futils.check_call', return_value=retcode):
            ips = devices.list_interface_ips(type, tap)
            futils.check_call.assert_called_once_with(["ip", "-6", "route", "list", "dev", tap])
            self.assertEqual(ips, set(["2001::"]))

        stdout = "2001:: scope link\n\n"
        retcode = futils.CommandOutput(stdout, "")
        with mock.patch('calico.felix.futils.check_call', return_value=retcode):
            ips = devices.list_interface_ips(type, tap)
            futils.check_call.assert_called_once_with(["ip", "-6", "route", "list", "dev", tap])
            self.assertEqual(ips, set(["2001::"]))

    def test_configure_interface_ipv4_mainline(self):
        m_open = mock.mock_open()
        tap = "tap" + str(uuid.uuid4())[:11]
        with mock.patch('__builtin__.open', m_open, create=True):
            devices.configure_interface_ipv4(tap)
        calls = [mock.call('/proc/sys/net/ipv4/conf/%s/route_localnet' % tap, 'wb'),
                 M_ENTER, mock.call().write('1'), M_CLEAN_EXIT,
                 mock.call('/proc/sys/net/ipv4/conf/%s/proxy_arp' % tap, 'wb'),
                 M_ENTER, mock.call().write('1'), M_CLEAN_EXIT,
                 mock.call('/proc/sys/net/ipv4/neigh/%s/proxy_delay' %tap, 'wb'),
                 M_ENTER, mock.call().write('0'), M_CLEAN_EXIT,]
        m_open.assert_has_calls(calls)

    def test_configure_interface_ipv6_mainline(self):
        """
        Test that configure_interface_ipv6_mainline
            - opens and writes to the /proc system to enable proxy NDP on the
              interface.
            - calls ip -6 neigh to set up the proxy targets.

        Mainline test has two proxy targets.
        """
        m_open = mock.mock_open()
        rc = futils.CommandOutput("", "")
        if_name = "tap3e5a2b34222"
        proxy_target = "2001::3:4"

        open_patch = mock.patch('__builtin__.open', m_open, create=True)
        m_check_call = mock.patch('calico.felix.futils.check_call',
                                  return_value=rc)

        with nested(open_patch, m_check_call) as (_, m_check_call):
            devices.configure_interface_ipv6(if_name, proxy_target)
            calls = [mock.call('/proc/sys/net/ipv6/conf/%s/proxy_ndp' %
                               if_name,
                               'wb'),
                     M_ENTER,
                     mock.call().write('1'),
                     M_CLEAN_EXIT]
            m_open.assert_has_calls(calls)
            ip_calls = [mock.call(["ip", "-6", "neigh", "add", "proxy",
                                   str(proxy_target), "dev", if_name])]
            m_check_call.assert_has_calls(ip_calls)

    def test_interface_up1(self):
        """
        Test that the interface_up returns True when an interface is up.
        """
        tap = "tap" + str(uuid.uuid4())[:11]

        with mock.patch('__builtin__.open') as open_mock:
            open_mock.return_value = mock.MagicMock(spec=file)
            file_handle = open_mock.return_value.__enter__.return_value
            file_handle.read.return_value = '0x1003\n'

            is_up = devices.interface_up(tap)

            open_mock.assert_called_with(
                '/sys/class/net/%s/flags' % tap, 'r'
            )
            self.assertTrue(file_handle.read.called)
            self.assertTrue(is_up)

    def test_interface_interface_up2(self):
        """
        Test that the interface_up returns False when an interface is down.
        """
        tap = "tap" + str(uuid.uuid4())[:11]

        with mock.patch('__builtin__.open') as open_mock:
            open_mock.return_value = mock.MagicMock(spec=file)
            file_handle = open_mock.return_value.__enter__.return_value
            file_handle.read.return_value = '0x1002\n'

            is_up = devices.interface_up(tap)

            open_mock.assert_called_with(
                '/sys/class/net/%s/flags' % tap, 'r'
            )
            self.assertTrue(file_handle.read.called)
            self.assertFalse(is_up)
