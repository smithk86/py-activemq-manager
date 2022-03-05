import asyncio
import logging
import socket
import time
from collections import namedtuple
from pathlib import Path
from uuid import uuid4

import docker
import pytest
import pytest_asyncio
import stomp

import docker_helpers
import activemq_manager


dir_ = Path(__file__).parent


# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
ContainerInfo = namedtuple("ContainerInfo", ["address", "port", "container"])


def pytest_addoption(parser):
    parser.addoption("--activemq-version", default="5.16.3")


@pytest.fixture(scope="session")
def activemq_version(request):
    return request.config.getoption("--activemq-version")


# override the default event_loop fixture
@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def activemq(activemq_version):
    client = docker.from_env()
    activemq_image = f"pytest-activemq:{activemq_version}"

    client.images.build(
        path=str(dir_.joinpath("activemq")),
        tag=activemq_image,
        buildargs={"ACTIVEMQ_VERSION": activemq_version},
    )

    with docker_helpers.run(
        activemq_image, ports=["8161/tcp", "61613/tcp"]
    ) as container_info:
        yield container_info


@pytest.fixture(scope="function")
def stomp_connection(activemq):
    client = stomp.Connection(
        host_and_ports=[("localhost", activemq.ports.get("61613/tcp"))]
    )

    logger.debug("waiting for stomp to connect to amq")
    while True:
        try:
            client.connect(wait=True)
            break
        except (
            OSError,
            stomp.exception.ConnectFailedException,
            stomp.exception.NotConnectedException,
        ):
            logger.debug("stomp connect failed...retry in 1s")
            time.sleep(1)

    yield client
    client.disconnect()


@pytest_asyncio.fixture(scope="function")
async def client(activemq):
    async with activemq_manager.Client(
        endpoint=f'http://localhost:{activemq.ports.get("8161/tcp")}',
        origin="http://pytest:80",
        auth=("admin", "admin"),
    ) as _client:
        yield _client


@pytest_asyncio.fixture(scope="function")
async def broker(client):
    _broker = client.broker()
    logger.debug("waiting for amq web interface")
    while True:
        try:
            await _broker.attribute("BrokerVersion")
            break
        except Exception as e:
            await asyncio.sleep(0.5)

    return _broker


@pytest_asyncio.fixture(scope="function")
async def load_messages(stomp_connection, broker, lorem_ipsum):
    stomp_connection.send(
        "pytest.queue1", str(uuid4()), test_prop1="abcd", test_prop2=3.14159
    )
    stomp_connection.send(
        "pytest.queue2", str(uuid4()), test_prop1="abcd", test_prop2=3.14159
    )
    stomp_connection.send(
        "pytest.queue2", str(uuid4()), test_prop1="abcd", test_prop2=3.14159
    )
    stomp_connection.send(
        "pytest.queue3", str(uuid4()), test_prop1="abcd", test_prop2=3.14159
    )
    stomp_connection.send(
        "pytest.queue3", str(uuid4()), test_prop1="abcd", test_prop2=3.14159
    )
    stomp_connection.send(
        "pytest.queue3", str(uuid4()), test_prop1="abcd", test_prop2=3.14159
    )
    stomp_connection.send(
        "pytest.queue4", lorem_ipsum, test_prop1="abcd", test_prop2=3.14159
    )
    stomp_connection.send(
        "pytest.queue4", lorem_ipsum, test_prop1="abcd", test_prop2=3.14159
    )
    stomp_connection.send(
        "pytest.queue4",
        lorem_ipsum,
        test_prop1="abcd",
        test_prop2=3.14159,
        headers={"persistent": "true"},
    )
    stomp_connection.send(
        "pytest.queue4",
        lorem_ipsum,
        test_prop1="abcd",
        test_prop2=3.14159,
        headers={"persistent": "true"},
    )
    await asyncio.sleep(1)

    yield

    async for q in broker.queues():
        await q.delete()


@pytest_asyncio.fixture(scope="function")
async def load_jobs(stomp_connection, broker):
    for _ in range(10):
        stomp_connection.send(
            "pytest.queue1", str(uuid4()), headers={"AMQ_SCHEDULED_DELAY": 100000000}
        )
    await asyncio.sleep(1)

    yield

    async for job in broker.jobs():
        await job.delete()


@pytest_asyncio.fixture(scope="session")
async def lorem_ipsum():
    with open(dir_.joinpath("files/lorem_ipsum.json")) as fh:
        return fh.read()
