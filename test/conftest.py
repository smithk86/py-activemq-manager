# add the project directory to the pythonpath
import os.path
import sys
from pathlib import Path
dir_ = Path(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, str(dir_.parent))


import asyncio
import logging
import socket
from collections import namedtuple
from time import sleep
from uuid import uuid4

import docker
import pytest
import stomp

import docker_helpers
import activemq_manager


#logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
ContainerInfo = namedtuple('ContainerInfo', ['address', 'port', 'container'])


def pytest_addoption(parser):
    parser.addoption('--activemq-version', required=True)


@pytest.fixture(scope='session')
def activemq_version(request):
    return request.config.getoption('--activemq-version')


# override the default event_loop fixture
@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope='session')
def activemq(activemq_version):
    dir_ = os.path.dirname(os.path.abspath(__file__))
    client = docker.from_env()
    activemq_image = f'test_activemq_manager:{activemq_version}'

    client.images.build(
        path=f'{dir_}/activemq',
        tag=activemq_image,
        buildargs={
            'ACTIVEMQ_VERSION': activemq_version
        }
    )

    container_info = docker_helpers.run(activemq_image, ports=['8161/tcp', '61613/tcp'])
    yield container_info
    container_info.container.stop()


@pytest.fixture(scope='function')
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
        except (OSError, stomp.exception.ConnectFailedException, stomp.exception.NotConnectedException):
            logger.debug('stomp connect failed...retry in 1s')
            sleep(1)

    yield client
    client.disconnect()


@pytest.fixture(scope='function')
@pytest.mark.asyncio
async def broker(activemq):
    broker_ = activemq_manager.Broker(
        endpoint=f'http://{activemq.address}:{activemq.ports.get("8161/tcp")}',
        origin='http://pytest:80',
        username='admin',
        password='admin'
    )

    logger.debug('waiting for amq web interface')
    while True:
        try:
            await broker_.attribute('BrokerVersion')
            break
        except Exception as e:
            sleep(1)

    return broker_


@pytest.mark.asyncio
@pytest.fixture(scope='function')
async def load_messages(stomp_connection, broker, lorem_ipsum):
    with open(f'{dir_}/files/lorem_ipsum.json') as fh:
        lorem_ipsum = fh.read()

    stomp_connection.send('pytest.queue1', str(uuid4()), test_prop1='abcd', test_prop2=3.14159)
    stomp_connection.send('pytest.queue2', str(uuid4()), test_prop1='abcd', test_prop2=3.14159)
    stomp_connection.send('pytest.queue2', str(uuid4()), test_prop1='abcd', test_prop2=3.14159)
    stomp_connection.send('pytest.queue3', str(uuid4()), test_prop1='abcd', test_prop2=3.14159)
    stomp_connection.send('pytest.queue3', str(uuid4()), test_prop1='abcd', test_prop2=3.14159)
    stomp_connection.send('pytest.queue3', str(uuid4()), test_prop1='abcd', test_prop2=3.14159)
    stomp_connection.send('pytest.queue4', lorem_ipsum,  test_prop1='abcd', test_prop2=3.14159)
    stomp_connection.send('pytest.queue4', lorem_ipsum,  test_prop1='abcd', test_prop2=3.14159)
    stomp_connection.send('pytest.queue4', lorem_ipsum,  test_prop1='abcd', test_prop2=3.14159, headers={'persistent': 'true'})
    stomp_connection.send('pytest.queue4', lorem_ipsum,  test_prop1='abcd', test_prop2=3.14159, headers={'persistent': 'true'})
    sleep(1)

    yield

    async for q in broker.queues():
        await q.delete()


@pytest.mark.asyncio
@pytest.fixture(scope='function')
async def load_jobs(stomp_connection, broker):
    for _ in range(10):
        stomp_connection.send('pytest.queue1', str(uuid4()), headers={
            'AMQ_SCHEDULED_DELAY': 100000000
        })
    sleep(1)

    yield

    async for job in broker.jobs():
        await job.delete()


@pytest.fixture(scope='session')
async def lorem_ipsum():
    with open(f'{dir_}/files/lorem_ipsum.json') as fh:
        return fh.read()
