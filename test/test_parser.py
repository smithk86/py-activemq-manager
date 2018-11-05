import logging
import os
import socket
from collections import namedtuple
from datetime import datetime
from time import sleep
from uuid import UUID, uuid4

import docker
import pytest
import stomp

from activemq_console_parser import ActiveMQError, Client, Connection, Queue, Message, MessageData, ScheduledMessage


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
            q.delete()
    request.addfinalizer(teardown)


@pytest.fixture(
    scope='class',
    params=[
        {
            'version': '5.15.6'
        }
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
        assert isinstance(self.parser.queue('pytest.queue1').to_dict(), dict)

        # assert the number of mesages
        assert self.parser.queue('pytest.queue1').messages_pending == 1
        assert self.parser.queue('pytest.queue2').messages_pending == 2
        assert self.parser.queue('pytest.queue3').messages_pending == 3
        assert self.parser.queue('pytest.queue4').messages_pending == 4

        # test removing queue1
        self.parser.queue('pytest.queue1').delete()
        with pytest.raises(ActiveMQError) as excinfo:
            self.parser.queue('pytest.queue1')
        assert 'queue not found' in str(excinfo.value)

    @pytest.mark.usefixtures('dataload')
    def test_messages(self):
        # remove one message from queue4 and re-check pending message count
        messages = self.parser.queue('pytest.queue4').messages()
        for _ in range(2):
            next(messages).delete()
        assert self.parser.queue('pytest.queue4').messages_pending == 2
        assert self.parser.queue('pytest.queue4').messages_dequeued == 2
        assert self.parser.queue('pytest.queue4').messages_enqueued == 4

        message = next(self.parser.queue('pytest.queue4').messages())
        data = message.data()
        assert type(data) is MessageData
        UUID(data.message) # ensure the message is a uuid
