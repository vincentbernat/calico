# Copyright (c) Metaswitch Networks 2015. All rights reserved.

import logging
import re
import sys
import os.path
import subprocess
import datetime
import uuid

_log = logging.getLogger(__name__)

# CONFIG PARAMS.
EGG_PATH = "eggs/"
BGP_PEER_IP = "172.18.203.239"
BGP_AS = 65530
PUBLIC_IP = "172.18.203.240"
CONTROLLER_PUBLIC_IP = PUBLIC_IP
PUBLIC_IPV6 = "2620:104:4001:194:250:56ff:fe92:1f50"
BGP_PEER_IPV6 = "2620:104:4001:194:250:56ff:fe92:3508"
CONTROLLER_HOSTNAME = "calico-rh01"


def run(args, fail_on_error=True):
    _log.info("Running %s", args)
    try:
        return subprocess.check_output(args)
    except subprocess.CalledProcessError as e:
        _log.error("Failed to run %s: %r", args, e)
        if fail_on_error:
            sys.exit(3)


NUM_NODES = 1000
HOSTNAME = run("hostname").strip()
CLUSTER_ID = uuid.uuid4().hex

ETCD_SYSCONFIG = \
"""
ETCD_DATA_DIR=/var/lib/etcd
ETCD_NAME={hostname}
ETCD_ADVERTISE_CLIENT_URLS="http://{public_ip}:2379,http://{public_ip}:4001"
ETCD_LISTEN_CLIENT_URLS="http://0.0.0.0:2379,http://0.0.0.0:4001"
ETCD_LISTEN_PEER_URLS="http://0.0.0.0:2380"
ETCD_INITIAL_ADVERTISE_PEER_URLS="http://{public_ip}:2380"
ETCD_INITIAL_CLUSTER_TOKEN="{cluster_id}"
ETCD_INITIAL_CLUSTER="{hostname}=http://{public_ip}:2380"
ETCD_INITIAL_CLUSTER_STATE=new
""".format(hostname=HOSTNAME,
           public_ip=PUBLIC_IP,
           cluster_id=CLUSTER_ID)

ETCD_PROXY_SYSCONFIG = \
"""
ETCD_PROXY=on
ETCD_DATA_DIR=/var/lib/etcd
ETCD_LISTEN_CLIENT_URLS="http://0.0.0.0:4001"
ETCD_INITIAL_CLUSTER="{cllr_hostname}=http://{cllr_ip}:2380"
""".format(cllr_hostname=CONTROLLER_HOSTNAME,
           cllr_ip=CONTROLLER_PUBLIC_IP)

ETCD_SERVICE = \
"""
[Unit]
Description=Etcd
After=syslog.target network.target

[Service]
User=root
ExecStart=/usr/local/bin/etcd
EnvironmentFile=-/etc/sysconfig/etcd
KillMode=process
Restart=always

[Install]
WantedBy=multi-user.target
"""



def backup_file(filename, now):
    backup_name = "%s.calico-%s" % (filename, now)
    run(["cp", filename, backup_name])
    return backup_name


def replace_config(lines, config_item, new_value):
    new_lines = []
    replacement_line = "%s = %s\n" % (config_item, new_value)
    found = False
    for line in lines:
        if re.match(r"\b%s\b.*=" % re.escape(config_item), line):
            line = replacement_line
            found = True
        new_lines.append(line)
    if not found:
        new_lines.append(replacement_line)
    return new_lines


def install_python_etcd():
    eggs = [
        "pycparser-2.12-py2.7.egg",
        "cffi-0.9.2-py2.7-linux-x86_64.egg",
        "enum34-1.0.4-py2.7.egg",
        "pyasn1-0.1.7-py2.7.egg",
        "six-1.9.0-py2.7.egg",
        "cryptography-0.8.2-py2.7-linux-x86_64.egg",
        "pyOpenSSL-0.15.1-py2.7.egg",
        "urllib3-1.7.1-py2.7.egg",
        "python_etcd-0.3.3_calico_2-py2.7.egg"
    ]
    # Make sure to install the eggs in the correct order
    for egg in eggs:
        run(["easy_install", EGG_PATH + egg])


def main():
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s [%(levelname)s] %(name)s %(lineno)d: %(message)s")
    now = datetime.datetime.now().isoformat()

    control_node = "control" in sys.argv

    run("yum-complete-transaction", fail_on_error=False)

    if control_node:
        # Replace Openstack RPMs with our own.
        _log.info("Installing control packages")
        _log.info("Updating Openstack with our packages")
        run(["yum", "update", "-y"])

        # Change ML2 config to use Calico driver.
        _log.info("Updating ML2 config.")
        filename = "/etc/neutron/plugins/ml2/ml2_conf.ini"
        bak_file = backup_file(filename, now)
        with open(filename, "r") as ml2_file:
            lines = ml2_file.readlines()
        lines = replace_config(lines, "type_drivers", "local, flat")
        lines = replace_config(lines, "tenant_network_types", "local")
        lines = replace_config(lines, "mechanism_drivers", "calico")
        with open(filename, "w") as ml2_file:
            ml2_file.writelines(lines)
        print run(["diff", bak_file, filename], fail_on_error=False)

        # Edit neutron config file.
        _log.info("Updating Neutron config.")
        filename = "/etc/neutron/neutron.conf"
        bak_file = backup_file(filename, now)
        with open(filename, "r") as f:
            lines = f.readlines()
        lines = replace_config(lines, "dhcp_agents_per_network", NUM_NODES)
        lines = replace_config(lines, "api_workers", 0)
        lines = replace_config(lines, "rpc_workers", 0)
        with open(filename, "w") as f:
            f.writelines(lines)
        print run(["diff", bak_file, filename], fail_on_error=False)

        # Install etcd.
        if not os.path.exists("etcd-v2.0.10-linux-amd64"):
            run(["tar", "xzf", "etcd-v2.0.10-linux-amd64.tar.gz"])
        if not os.path.exists("/usr/local/bin/etcd"):
            run(["mv", "etcd-v2.0.10-linux-amd64/etcd", "/usr/local/bin"])
            run(["mv", "etcd-v2.0.10-linux-amd64/etcdctl", "/usr/local/bin"])
        with open("/etc/passwd", "r") as passwd:
            if "etcd" not in passwd.read():
                _log.info("etcd user didn't exist, creating...")
                run(["adduser", "-s", "/sbin/nologin", "-d", "/var/lib/etcd/", "etcd"])
        run(["chmod", "700", "/var/lib/etcd/"])

        create_ram_disk = False
        with open("/etc/fstab", "r") as fstab:
            if "/var/lib/etcd" not in fstab.read():
                _log.info("Creating RAM disk for etcd")
                create_ram_disk = True
        if create_ram_disk:
            with open("/etc/fstab", "a") as fstab:
                fstab.write("\ntmpfs /var/lib/etcd tmpfs "
                            "nodev,nosuid,noexec,nodiratime,size=512M 0 0\n")
                fstab.flush()
                run(["mount", "-a"])
        if "/var/lib/etcd" not in run(["mount"]):
            _log.error("Failed to mount etcd RAM disk.")
            sys.exit(2)

        with open("/etc/sysconfig/etcd", "w") as sf:
            sf.write(ETCD_SYSCONFIG)
        with open("/usr/lib/systemd/system/etcd.service", "w") as f:
            f.write(ETCD_SERVICE)

        run(["systemctl", "start", "etcd"])
        run(["systemctl", "enable", "etcd"])

        install_python_etcd()

        run(["yum", "install", "-y", "calico-control"])
        run(["service", "neutron-server", "restart"])

    if "compute" in sys.argv:
        _log.info("Installing compute services")
        run(["setenforce", "permissive"])


        _log.info("Updating SELINUX config.")
        filename = "/etc/selinux/config"
        bak_file = backup_file(filename, now)
        with open(filename, "r") as f:
            lines = f.readlines()
        lines = replace_config(lines, "SELINUX", "permissive")
        with open(filename, "w") as f:
            f.writelines(lines)
        print run(["diff", bak_file, filename], fail_on_error=False)


        _log.info("Updating qemu config.")
        filename = "/etc/libvirt/qemu.conf"
        bak_file = backup_file(filename, now)
        with open(filename, "r") as f:
            lines = f.readlines()
        lines = replace_config(lines, "clear_emulator_capabilities", "0")
        lines = replace_config(lines, "user", "root")
        lines = replace_config(lines, "group", "root")
        lines = replace_config(
            lines,
            "cgroup_device_acl",
            '['
            '"/dev/null", "/dev/full", "/dev/zero", '
            '"/dev/random", "/dev/urandom", '
            '"/dev/ptmx", "/dev/kvm", "/dev/kqemu", '
            '"/dev/rtc", "/dev/hpet", "/dev/net/tun", '
            ']'
        )
        with open(filename, "w") as f:
            f.writelines(lines)
        print run(["diff", bak_file, filename], fail_on_error=False)

        run(["service", "libvirtd", "restart"])


        _log.info("Updating nova config.")
        filename = "/etc/libvirt/qemu.conf"
        bak_file = backup_file(filename, now)
        with open(filename, "r") as f:
            lines = f.readlines()
        lines = replace_config(lines, "linuxnet_interface_driver", None)
        lines = replace_config(lines, "service_neutron_metadata_proxy", None)
        lines = replace_config(lines, "service_metadata_proxy", None)
        lines = replace_config(lines, "metadata_proxy_shared_secret", None)
        with open(filename, "w") as f:
            f.writelines(lines)
        print run(["diff", bak_file, filename], fail_on_error=False)

        run(["service", "openstack-nova-compute", "restart"])
        if control_node:
            run(["service", "openstack-nova-api", "restart"])

        if not control_node:
            run(["yum", "update", "-y"])

        run(["yum", "install", "openstack-neutron"])

        run(["service", "neutron-openvswitch-agent", "stop"], fail_on_error=False)
        run(["service", "openvswitch", "stop"], fail_on_error=False)
        run(["chkconfig", "openvswitch", "off"], fail_on_error=False)
        run(["chkconfig", "neutron-openvswitch-agent", "off"], fail_on_error=False)

        _log.info("Updating dhcp config.")
        filename = "/etc/neutron/dhcp_agent.ini"
        bak_file = backup_file(filename, now)
        with open(filename, "r") as f:
            lines = f.readlines()
        if any(["interface_driver" in l for l in lines]):
            lines = replace_config(lines, "interface_driver",
                               "neutron.agent.linux.interface.RoutedInterfaceDriver")
        else:
            for i in range(len(lines)):
                if "[DEFAULT]" in lines[i]:
                    lines[i:i] = ["interface_driver = "
                                  "neutron.agent.linux.interface.RoutedInterfaceDriver"]
                    break
        with open(filename, "w") as f:
            f.writelines(lines)
        print run(["diff", bak_file, filename], fail_on_error=False)

        run(["service", "neutron-dhcp-agent", "restart"])
        run(["chkconfig", "neutron-dhcp-agent", "on"])
        run(["service", "neutron-l3-agent", "stop"])
        run(["chkconfig", "neutron-l3-agent", "off"])

        if not control_node:
            run(["yum", "install", "-y", "openstack-nova-api"])
            run(["service", "openstack-nova-metadata-api", "restart"])
            run(["chkconfig", "openstack-nova-metadata-api", "on"])

        run(["yum", "install", "bird", "bird6"])

        if not control_node:
            # Install etcd.
            if not os.path.exists("etcd-v2.0.10-linux-amd64"):
                run(["tar", "xzf", "etcd-v2.0.10-linux-amd64.tar.gz"])
            if not os.path.exists("/usr/local/bin/etcd"):
                run(["mv", "etcd-v2.0.10-linux-amd64/etcd", "/usr/local/bin"])
                run(["mv", "etcd-v2.0.10-linux-amd64/etcdctl", "/usr/local/bin"])
            with open("/etc/passwd", "r") as passwd:
                if "etcd" not in passwd.read():
                    _log.info("etcd user didn't exist, creating...")
                    run(["adduser", "-s", "/sbin/nologin", "-d", "/var/lib/etcd/", "etcd"])
            run(["chmod", "700", "/var/lib/etcd/"])

            create_ram_disk = False
            with open("/etc/fstab", "r") as fstab:
                if "/var/lib/etcd-rd" not in fstab.read():
                    _log.info("Creating RAM disk for etcd")
                    create_ram_disk = True
            if create_ram_disk:
                with open("/etc/fstab", "a") as fstab:
                    fstab.write("\ntmpfs /var/lib/etcd tmpfs "
                                "nodev,nosuid,noexec,nodiratime,size=512M 0 0\n")
                    run(["mount", "-a"])
            if "/var/lib/etcd" not in run(["mount"]):
                _log.error("Failed to mount etcd RAM disk.")
                sys.exit(2)

            with open("/etc/sysconfig/etcd", "w") as sf:
                sf.write(ETCD_PROXY_SYSCONFIG)
            with open("/usr/lib/systemd/system/etcd.service", "w") as f:
                f.write(ETCD_SERVICE)

            run(["systemctl", "start", "etcd"])
            run(["systemctl", "enable", "etcd"])

            install_python_etcd()

        run(["/usr/bin/calico-gen-bird-conf.sh", PUBLIC_IP, BGP_PEER_IP, BGP_AS])
        run(["/usr/bin/calico-gen-bird6-conf.sh", PUBLIC_IP, PUBLIC_IPV6, BGP_PEER_IPV6, BGP_AS])
        run(["service", "bird", "restart"])
        run(["chkconfig", "bird", "on"])
        run(["service", "bird6", "restart"])
        run(["chkconfig", "bird6", "on"])


if __name__ == "__main__":
    main()