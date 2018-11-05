import os
from datetime import datetime

import pytest
import requests
import requests_mock

from activemq_console_parser import Client, Connection, Queue, Message, ScheduledMessage


def skip_mock_server(server):
    if server['url'].startswith('mock'):
        pytest.skip('mock adapter')


@pytest.fixture(scope='class')
def client(request):

    module_directory = os.path.abspath(os.path.dirname(__file__))

    client = Client(
        host='localhost',
        protocol='mock'
    )

    mock_adapter = requests_mock.Adapter()

    def add_mock_route(route):
        with open(f'{module_directory}/data/{route}') as fh:
            mock_data = fh.read()
        mock_adapter.register_uri('GET', f'/admin/{route}', text=mock_data)

    add_mock_route('queues.jsp')
    add_mock_route('browse.jsp')
    add_mock_route('connections.jsp')
    add_mock_route('scheduled.jsp')

    client.session.mount('mock', mock_adapter)

    def fin():
        client.close()

    request.cls.client = client


@pytest.mark.usefixtures('client')
class TestClient():

    def skip_mock_server(self):
        skip_mock_server(self.server)

    def test_scheduled_messages(self):
        for m in self.client.scheduled_messages():
            assert type(m) is ScheduledMessage
            assert type(m.client) is Client
            assert type(m.message_id) is str
            assert type(m.next_scheduled_time) is datetime
            assert type(m.start) is datetime
            assert type(m.delay) is int
            assert type(m.href_delete) is str

    def test_connections(self):
        for c in self.client.connections():
            assert type(c) is Connection
            assert isinstance(c._asdict(), dict)
            assert type(c.id) is str
            assert type(c.id_href) is str
            assert type(c.remote_address) is str
            assert type(c.active) is bool
            assert type(c.slow) is bool

    def test_queues(self):
        for q in self.client.queues():
            assert type(q) is Queue
            assert type(q.to_dict()) is dict
            assert type(q.client) is Client
            assert type(q.name) is str
            assert type(q.messages_pending) is int
            assert type(q.messages_enqueued) is int
            assert type(q.messages_dequeued) is int
            assert type(q.consumers) is int
            assert type(q.href_purge) is str
            assert type(q.href_delete) is str

    def test_messages(self):
        q = next(self.client.queues())
        for m in q.messages():
            assert type(m) is Message
            assert type(m.message_id) is str
            assert type(m.href_properties) is str
            assert type(m.persistence) is bool
            assert type(m.timestamp) is datetime
            assert type(m.href_delete) is str
