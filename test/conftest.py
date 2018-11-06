import logging
import os.path
import socket
from collections import namedtuple
from time import sleep
from uuid import uuid4

import docker
import pytest
import stomp

from activemq_console_parser import Client


logger = logging.getLogger(__name__)
ContainerInfo = namedtuple('ContainerInfo', ['address', 'port', 'container'])


def pytest_addoption(parser):
    parser.addoption('--activemq-version', required=True, default='latest')


@pytest.fixture(scope='module')
def activemq(request):
    dir_ = os.path.dirname(os.path.abspath(__file__))
    client = docker.from_env()
    version = request.config.getoption('--activemq-version')
    tag = f'pytest_activemq:{version}'

    client.images.build(
        path=f'{dir_}/activemq',
        tag=tag,
        buildargs={
            'ACTIVEMQ_VERSION': version
        }
    )

    container = client.containers.run(
        image=tag,
        detach=True,
        auto_remove=True,
        ports={
            '8161/tcp': ('127.0.0.1', None),
            '61613/tcp': ('127.0.0.1', None)
        }
    )
    ports = client.api.inspect_container(container.id)['NetworkSettings']['Ports']
    web_port = ports['8161/tcp'][0]
    stomp_port = ports['61613/tcp'][0]

    def teardown():
        container.stop()
    request.addfinalizer(teardown)

    return ContainerInfo(
        address=web_port['HostIp'],
        port=(int(web_port['HostPort']), int(stomp_port['HostPort'])),
        container=container
    )


@pytest.fixture(scope='function')
def stomp_connection(request, activemq):
    web_port, stomp_port = activemq.port
    client = stomp.Connection(
        host_and_ports=[
            (activemq.address, stomp_port)
        ]
    )

    logger.debug('waiting for stomp to connect to amq')
    while True:
        try:
            client.connect(wait=True)
            break
        except stomp.exception.ConnectFailedException:
            logger.debug('stomp connect failed...retry in 1s')
            sleep(1)

    def teardown():
        client.disconnect()
    request.addfinalizer(teardown)

    return client


@pytest.fixture(scope='function')
def console_parser(request, activemq):
    web_port, stomp_port = activemq.port
    client = Client(
        host=activemq.address,
        port=web_port,
        username='admin',
        password='admin'
    )

    logger.debug('waiting for amq web interface')
    while True:
        try:
            client.get('/admin/queues.jsp')
            break
        except Exception:
            sleep(1)

    def teardown():
        client.close()
    request.addfinalizer(teardown)

    return client


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
