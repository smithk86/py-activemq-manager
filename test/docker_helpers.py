import asyncio
from collections import namedtuple

import docker


ContainerInfo = namedtuple('ContainerInfo', ['address', 'ports', 'container'])


async def wait_for_port(address, port, timeout=5):
    async def port_is_available():
        writer = None
        try:
            _, writer = await asyncio.open_connection(address, port)
            return True
        except ConnectionRefusedError:
            return False
        finally:
            if writer:
                writer.close()

    async def loop():
        while True:
            if await port_is_available():
                break

    try:
        await asyncio.wait_for(loop(), timeout=5)
        return True
    except asyncio.TimeoutError:
        return False


def run(image, ports, protocol='tcp', address='127.0.0.1'):
    container_run_ports = dict()
    for port in ports:
        container_run_ports[port] = (address, None)
    client = docker.from_env()
    container = client.containers.run(
        image,
        detach=True,
        auto_remove=True,
        ports=container_run_ports
    )

    _ports = client.api.inspect_container(container.id)['NetworkSettings']['Ports']
    _first_port = ports[0]
    _address = _ports[_first_port][0]['HostIp']
    _binded_ports = dict()
    for port in ports:
        _binded_ports[port] = int(_ports[port][0]['HostPort'])
    for port in _binded_ports.values():
        if asyncio.run(wait_for_port(_address, port, timeout=5)) is False:
            raise RuntimeException('httpd did not start within 5s')

    return ContainerInfo(
        address=_address,
        ports=_binded_ports,
        container=container
    )
