import logging
from datetime import datetime
from uuid import UUID


import pytest

from activemq_console_parser import ActiveMQError, Client, Connection, Queue, Message, MessageData, ScheduledMessage


logging.basicConfig(level=logging.INFO)


@pytest.mark.usefixtures('load_messages')
def test_connections(console_parser):
    assert sum(1 for i in console_parser.connections()) == 1
    for c in console_parser.connections():
        assert type(c) is Connection
        assert isinstance(c._asdict(), dict)
        assert type(c.id) is str
        assert type(c.id_href) is str
        assert type(c.remote_address) is str
        assert type(c.active) is bool
        assert type(c.slow) is bool


@pytest.mark.usefixtures('load_messages')
def test_queues(console_parser):
    for q in console_parser.queues():
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

    # assert the number of mesages in each queue
    assert console_parser.queue('pytest.queue1').messages_pending == 1
    assert console_parser.queue('pytest.queue2').messages_pending == 2
    assert console_parser.queue('pytest.queue3').messages_pending == 3
    assert console_parser.queue('pytest.queue4').messages_pending == 4

    # test Queue.delete() with queue1
    console_parser.queue('pytest.queue1').delete()
    with pytest.raises(ActiveMQError) as excinfo:
        console_parser.queue('pytest.queue1')
    assert 'queue not found' in str(excinfo.value)

    # test Queue.purge() with queue4
    console_parser.queue('pytest.queue4').purge()
    assert console_parser.queue('pytest.queue4').messages_pending == 0


@pytest.mark.usefixtures('load_messages')
def test_messages(console_parser):

    # validate all messages
    for m in console_parser.queue('pytest.queue4').messages():
        assert type(m) is Message
        assert type(m.message_id) is str
        assert type(m.href_properties) is str
        assert type(m.persistence) is bool
        assert type(m.timestamp) is datetime
        assert type(m.href_delete) is str

        data = m.data()
        assert type(data) is MessageData
        UUID(data.message) # ensure the message is a uuid

    # test Message.delete()
    messages = console_parser.queue('pytest.queue4').messages()
    for _ in range(2):
        next(messages).delete()
    assert console_parser.queue('pytest.queue4').messages_pending == 2
    assert console_parser.queue('pytest.queue4').messages_dequeued == 2
    assert console_parser.queue('pytest.queue4').messages_enqueued == 4


@pytest.mark.usefixtures('load_scheduled_messages')
def test_scheduled_messages(console_parser, stomp_connection):
    # validate number of scheduled message
    assert console_parser.scheduled_messages_count() == 10
    # validate messages
    for m in console_parser.scheduled_messages():
        assert type(m) is ScheduledMessage
        assert type(m.client) is Client
        assert type(m.message_id) is str
        assert type(m.next_scheduled_time) is datetime
        assert type(m.start) is datetime
        assert type(m.delay) is int
        assert type(m.href_delete) is str
