import os
from datetime import datetime

import pytest
import requests
import requests_mock

from activemq_console_parser import Client, Connection, Queue, Message, ScheduledMessage


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

    def test_scheduled_messages(self):
        for m in self.client.scheduled_messages():
            assert type(m) is ScheduledMessage
            assert type(m.client) is Client
            assert type(m.message_id) is str
            assert type(m.next_scheduled_time) is datetime
            assert type(m.start) is datetime
            assert type(m.delay) is int
            assert type(m.href_delete) is str
