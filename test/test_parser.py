import logging
import os
import socket
from collections import namedtuple
from datetime import datetime
from time import sleep
from uuid import uuid4

import docker
import pytest
import stomp

from activemq_console_parser import Client, Connection, Queue, Message, ScheduledMessage


ContainerInfo = namedtuple('ContainerInfo', ['address', 'port', 'container'])
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def activemq_instance(version='latest'):
    dir_ = os.path.dirname(os.path.abspath(__file__))
    client = docker.from_env()

    container = client.containers.run(
        f'rmohr/activemq:{version}',
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

    return ContainerInfo(
        address=web_port['HostIp'],
        port=(int(web_port['HostPort']), int(stomp_port['HostPort'])),
        container=container
    )


@pytest.fixture(scope='function')
def dataload(request, client):
    parser, stomp = client
    stomp.send('pytest.queue1', str(uuid4()))
    stomp.send('pytest.queue2', str(uuid4()))
    stomp.send('pytest.queue2', str(uuid4()))
    stomp.send('pytest.queue3', str(uuid4()))
    stomp.send('pytest.queue3', str(uuid4()))
    stomp.send('pytest.queue3', str(uuid4()))
    stomp.send('pytest.queue4', str(uuid4()))
    stomp.send('pytest.queue4', str(uuid4()))
    stomp.send('pytest.queue4', str(uuid4()))
    stomp.send('pytest.queue4', str(uuid4()))
    sleep(1)

    def teardown():
        for q in parser.queues():
            parser.get(f'/admin/{q.href_delete}')
    request.addfinalizer(teardown)


@pytest.fixture(
    scope='class',
    params=[
        {
            'version': '5.15.6'
        },
        {
            'version': '5.13.4'
        },
    ]
)
def client(request):
    server = request.param
    container_info = activemq_instance(server['version'])
    web_port, stomp_port = container_info.port

    request.cls.stomp = stomp.Connection(
        host_and_ports=[
            (container_info.address, stomp_port)
        ]
    )
    while True:
        try:
            request.cls.stomp.connect(wait=True)
            break
        except stomp.exception.ConnectFailedException:
            logger.debug('stomp connect failed...retry in 1s')
            sleep(1)

    request.cls.parser = Client(
        host=container_info.address,
        port=web_port,
        username='admin',
        password='admin'
    )
    logger.debug('waiting for amq web interface')
    while True:
        try:
            request.cls.parser.get('/admin/queues.jsp')
            break
        except Exception:
            sleep(1)

    def teardown():
        request.cls.parser.close()
        request.cls.stomp.disconnect()
        container_info.container.stop()
    request.addfinalizer(teardown)

    return (request.cls.parser, request.cls.stomp)


@pytest.mark.usefixtures('client')
class TestClient():

    @pytest.mark.usefixtures('dataload')
    def test_queues(self):
        assert self.parser.queue('pytest.queue1').messages_pending == 1
        assert self.parser.queue('pytest.queue2').messages_pending == 2
        assert self.parser.queue('pytest.queue3').messages_pending == 3
        assert self.parser.queue('pytest.queue4').messages_pending == 4

    # def test_scheduled_messages(self):
    #     for m in self.client.scheduled_messages():
    #         assert type(m) is ScheduledMessage
    #         assert type(m.client) is Client
    #         assert type(m.message_id) is str
    #         assert type(m.next_scheduled_time) is datetime
    #         assert type(m.start) is datetime
    #         assert type(m.delay) is int
    #         assert type(m.href_delete) is str

    # def test_connections(self):
    #     for c in self.client.connections():
    #         assert type(c) is Connection
    #         assert isinstance(c._asdict(), dict)
    #         assert type(c.id) is str
    #         assert type(c.id_href) is str
    #         assert type(c.remote_address) is str
    #         assert type(c.active) is bool
    #         assert type(c.slow) is bool

    # def test_queues(self):
    #     for q in self.client.queues():
    #         assert type(q) is Queue
    #         assert type(q.to_dict()) is dict
    #         assert type(q.client) is Client
    #         assert type(q.name) is str
    #         assert type(q.messages_pending) is int
    #         assert type(q.messages_enqueued) is int
    #         assert type(q.messages_dequeued) is int
    #         assert type(q.consumers) is int
    #         assert type(q.href_purge) is str

    # def test_messages(self):
    #     q = next(self.client.queues())
    #     for m in q.messages():
    #         assert type(m) is Message
    #         assert type(m.message_id) is str
    #         assert type(m.persistence) is bool
    #         assert type(m.timestamp) is datetime
    #         assert type(m.href_properties) is str
    #         assert type(m.href_delete) is str
