import subprocess
import os

import docker

from mininet.net import Mininet
from mininet.node import Host, Controller, OVSSwitch, Node, OVSController
from mininet.link import TCLink
from mininet.log import setLogLevel, info, debug, error
from mininet.clean import cleanup
from mininet.cli import CLI
from mininet.topo import LinearTopo, Topo


class LinuxRouter(Node):
    """"A Node with IP forwarding enabled."""

    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        # Enable forwarding on the router
        self.cmd('sysctl net.ipv4.ip_forward=1')

    def terminate(self):
        self.cmd('sysctl net.ipv4.ip_forward=0')
        super(LinuxRouter, self).terminate()


class NetworkTopo(Topo):
    "A LinuxRouter connecting three IP subnets"

    def build(self, **opts):
        default_ip = '10.0.0.1/24'  # IP address for r0-eth1
        router = self.addNode('r0', cls=LinuxRouter, ip=default_ip)

        s1 = self.addSwitch('s1')

        filesystem1 = self.addHost('fs1', ip='10.0.0.2', defaultRoute='via 10.0.0.1')
        worker1 = self.addHost('worker1', ip='10.0.0.3', defaultRoute='via 10.0.0.1')
        worker2 = self.addHost('worker2', ip='10.0.0.4', defaultRoute='via 10.0.0.1')
        worker3 = self.addHost('worker3', ip='10.0.0.5', defaultRoute='via 10.0.0.1')
        client1 = self.addHost('client1', ip='10.0.0.6', defaultRoute='via 10.0.0.1')
        filesystem2 = self.addHost('fs2', ip='10.0.0.7', defaultRoute='via 10.0.0.1')

        self.addLink(s1, router, intfName2='r0-eth1', params2={'ip': default_ip})

        self.addLink(filesystem1, s1)
        self.addLink(filesystem2, s1)
        self.addLink(worker1, s1)
        self.addLink(worker2, s1)
        self.addLink(worker3, s1)
        self.addLink(client1, s1)

topos = { 'topo': (lambda: NetworkTopo()) }