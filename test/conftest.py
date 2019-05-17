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
from activemq_console_parser import Client


logger = logging.getLogger(__name__)
ContainerInfo = namedtuple('ContainerInfo', ['address', 'port', 'container'])


def pytest_addoption(parser):
    parser.addoption('--activemq-version', help='IE: 5.13.4, 5.15.7', required=True, default='latest')


@pytest.fixture(scope='session')
def activemq_version(request):
    return request.config.getoption('--activemq-version')


@pytest.yield_fixture(scope='session')
def activemq(activemq_version):
    dir_ = os.path.dirname(os.path.abspath(__file__))
    client = docker.from_env()
    image_name = f'pytest_activemq:{activemq_version}'

    client.images.build(
        path=f'{dir_}/activemq',
        tag=image_name,
        buildargs={
            'ACTIVEMQ_VERSION': activemq_version
        }
    )

    container_info = docker_helpers.run(image_name, ports=['8161/tcp', '61613/tcp'])
    yield container_info
    container_info.container.stop()


@pytest.yield_fixture(scope='function')
def stomp_connection(request, activemq):
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
def console_parser(request, activemq):
    client = Client(
        endpoint=f'http://{activemq.address}:{activemq.ports.get("8161/tcp")}',
        username='admin',
        password='admin'
    )

    logger.debug('waiting for amq web interface')
    while True:
        try:
            client.web('/admin/queues.jsp')
            break
        except Exception as e:
            sleep(1)

    yield client
    client.close()


@pytest.fixture(scope='function')
def load_messages(request, stomp_connection, console_parser):
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

    def teardown():
        for q in console_parser.queues():
            q.delete()
    request.addfinalizer(teardown)


@pytest.fixture(scope='function')
def load_scheduled_messages(request, stomp_connection, console_parser):
    for _ in range(10):
        stomp_connection.send('pytest.queue1', str(uuid4()), headers={
            'AMQ_SCHEDULED_DELAY': 100000000
        })
    sleep(1)

    def teardown():
        for q in console_parser.scheduled_messages():
            q.delete()
    request.addfinalizer(teardown)
