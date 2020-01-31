import asyncio
import os
import re
import socket
import time
from collections import namedtuple

import docker


ParsedDockerHost = namedtuple('ParsedDockerHost', ['protocol', 'hostname', 'port'])
ContainerInfo = namedtuple('ContainerInfo', ['address', 'ports', 'container'])


def parse_docker_host():
    DOCKER_HOST = os.environ.get('DOCKER_HOST')
    m = re.match(r'([\w]*)://([\w\.]*):([\d]*)', DOCKER_HOST)
    if m:
        return ParsedDockerHost(
            protocol=m.group(1),
            hostname=m.group(2),
            port=int(m.group(3))
        )
    else:
        return ParsedDockerHost(
            protocol='tcp',
            hostname='localhost',
            port=2375
        )


def wait_for_port(host, port, timeout=5):
    start_time = time.perf_counter()
    while True:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except OSError as ex:
            time.sleep(0.01)
            if time.perf_counter() - start_time >= timeout:
                return False


def run(image, ports):
    container_run_ports = dict()
    for port in ports:
        container_run_ports[port] = ('0.0.0.0', None)
    parsed_host = parse_docker_host()
    client = docker.from_env()
    container = client.containers.run(
        image,
        detach=True,
        auto_remove=True,
        ports=container_run_ports
    )

    _ports = client.api.inspect_container(container.id)['NetworkSettings']['Ports']
    _first_port = ports[0]
    _binded_ports = dict()
    for port in ports:
        _binded_ports[port] = int(_ports[port][0]['HostPort'])
    for port in _binded_ports.values():
        if wait_for_port(parsed_host.hostname, port, timeout=15) is False:
            raise RuntimeError('httpd did not start within 15s')

    return ContainerInfo(
        address=parsed_host.hostname,
        ports=_binded_ports,
        container=container
    )
