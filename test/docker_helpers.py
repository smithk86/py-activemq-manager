import asyncio
import socket
import time
from collections import namedtuple
from contextlib import contextmanager

import docker  # type: ignore


ContainerInfo = namedtuple("ContainerInfo", ["ports", "container"])


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


@contextmanager
def run(image, ports):
    container_run_ports = dict()
    for port in ports:
        container_run_ports[port] = ("0.0.0.0", None)
    client = docker.from_env()
    container = client.containers.run(
        image, detach=True, auto_remove=True, ports=container_run_ports
    )

    _ports = client.api.inspect_container(container.id)["NetworkSettings"]["Ports"]
    _first_port = ports[0]
    _binded_ports = dict()
    for port in ports:
        _binded_ports[port] = int(_ports[port][0]["HostPort"])
    for port in _binded_ports.values():
        if wait_for_port("localhost", port, timeout=15) is False:
            raise RuntimeError("httpd did not start within 15s")

    try:
        yield ContainerInfo(ports=_binded_ports, container=container)
    finally:
        container.stop()
