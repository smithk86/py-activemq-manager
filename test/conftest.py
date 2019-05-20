import asyncio
import logging
import os.path
import socket
from collections import namedtuple
from time import sleep
from uuid import uuid4

import docker
import pytest
import stomp

import docker_helpers
import activemq_manager


logger = logging.getLogger(__name__)
ContainerInfo = namedtuple('ContainerInfo', ['address', 'port', 'container'])


def pytest_addoption(parser):
    parser.addoption('--activemq-image', required=True)


@pytest.fixture(scope='session')
def activemq_image(request):
    return request.config.getoption('--activemq-image')


# override the default event_loop fixture
@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.yield_fixture(scope='session')
def activemq(activemq_image):
    dir_ = os.path.dirname(os.path.abspath(__file__))
    # client = docker.from_env()

    # client.images.build(
    #     path=f'{dir_}/activemq',
    #     tag=activemq_image,
    #     buildargs={
    #         'ACTIVEMQ_VERSION': activemq_version
    #     }
    # )

    container_info = docker_helpers.run(activemq_image, ports=['8161/tcp', '61613/tcp'])
    yield container_info
    container_info.container.stop()


@pytest.yield_fixture(scope='function')
def stomp_connection(activemq):
    client = stomp.Connection(
        host_and_ports=[
            (activemq.address, activemq.ports.get('61613/tcp'))
        ]
    )

    logger.debug('waiting for stomp to connect to amq')
    while True:
        try:
            client.connect(wait=True)
            break
        except (stomp.exception.ConnectFailedException, stomp.exception.NotConnectedException):
            logger.debug('stomp connect failed...retry in 1s')
            sleep(1)

    yield client
    client.disconnect()


@pytest.yield_fixture(scope='function')
@pytest.mark.asyncio
async def console_parser(activemq):
    client = activemq_manager.Client(
        endpoint=f'http://{activemq.address}:{activemq.ports.get("8161/tcp")}',
        username='admin',
        password='admin'
    )

    logger.debug('waiting for amq web interface')
    while True:
        try:
            await client.web('/admin/queues.jsp')
            break
        except Exception as e:
            sleep(1)

    yield client
    await client.close()


@pytest.mark.asyncio
@pytest.yield_fixture(scope='function')
async def load_messages(stomp_connection, console_parser):
    stomp_connection.send('pytest.queue1', str(uuid4()))
    stomp_connection.send('pytest.queue2', str(uuid4()))
    stomp_connection.send('pytest.queue2', str(uuid4()))
    stomp_connection.send('pytest.queue3', str(uuid4()))
    stomp_connection.send('pytest.queue3', str(uuid4()))
    stomp_connection.send('pytest.queue3', str(uuid4()))
    stomp_connection.send('pytest.queue4', str(uuid4()))
    stomp_connection.send('pytest.queue4', str(uuid4()))
    stomp_connection.send('pytest.queue4', str(uuid4()))
    stomp_connection.send('pytest.queue4', str(uuid4()))
    sleep(1)

    yield

    async for q in console_parser.queues():
        await q.delete()


@pytest.mark.asyncio
@pytest.yield_fixture(scope='function')
async def load_scheduled_messages(stomp_connection, console_parser):
    for _ in range(10):
        stomp_connection.send('pytest.queue1', str(uuid4()), headers={
            'AMQ_SCHEDULED_DELAY': 100000000
        })
    sleep(1)

    yield

    async for q in console_parser.scheduled_messages():
        await q.delete()
