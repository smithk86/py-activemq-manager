from datetime import datetime
from uuid import UUID

import pytest

from activemq_manager import BrokerError, Client, Connection, Queue, Message, MessageData, ScheduledMessage


async def alist(aiter_):
    list_ = list()
    async for x in aiter_:
        list_.append(x)
    return list_


# these tests cannot be used as STOMP does not keep an open connections
# @pytest.mark.asyncio
# @pytest.mark.usefixtures('load_messages')
# async def test_connections(console_parser):
#     connections = await alist(console_parser.connections())
#     assert len(connections) > 0
#     for c in connections:
#         assert type(c) is Connection
#         assert type(c.client) is Client
#         assert isinstance(c._asdict(), dict)
#         assert type(c.id) is str
#         assert type(c.remote_address) is str
#         assert type(c.active) is bool
#         assert type(c.slow) is bool


@pytest.mark.usefixtures('load_messages')
@pytest.mark.asyncio
async def test_queues(console_parser):
    async for q in console_parser.queues():
        assert type(q) is Queue
        assert type(q.asdict()) is dict
        assert type(q.client) is Client
        assert type(q.name) is str
        assert type(q.messages_pending) is int
        assert type(q.messages_enqueued) is int
        assert type(q.messages_dequeued) is int
        assert type(q.consumers) is int

    # assert the number of mesages in each queue
    assert (await console_parser.queue('pytest.queue1')).messages_pending == 1
    assert (await console_parser.queue('pytest.queue2')).messages_pending == 2
    assert (await console_parser.queue('pytest.queue3')).messages_pending == 3
    assert (await console_parser.queue('pytest.queue4')).messages_pending == 4

    # test Queue.delete() with queue1
    await (await console_parser.queue('pytest.queue1')).delete()
    with pytest.raises(BrokerError) as excinfo:
        await console_parser.queue('pytest.queue1')
    assert 'queue not found: pytest.queue1' in str(excinfo.value)

    # test Queue.purge() with queue4
    await (await console_parser.queue('pytest.queue4')).purge()
    assert (await console_parser.queue('pytest.queue4')).messages_pending == 0


@pytest.mark.asyncio
@pytest.mark.usefixtures('load_messages')
async def test_messages(console_parser):
    # validate all messages
    async for m in (await console_parser.queue('pytest.queue4')).messages():
        assert type(m) is Message
        assert type(m.message_id) is str
        assert type(m.persistence) is bool
        assert type(m.timestamp) is datetime

        data = await m.data()
        assert type(data) is MessageData
        UUID(data.message)  # ensure the message is a uuid

    # test Message.delete()
    messages = (await console_parser.queue('pytest.queue4')).messages()
    for _ in range(2):
        msg = await messages.__anext__()
        await msg.delete()
    assert (await console_parser.queue('pytest.queue4')).messages_pending == 2
    assert (await console_parser.queue('pytest.queue4')).messages_dequeued == 2
    assert (await console_parser.queue('pytest.queue4')).messages_enqueued == 4


@pytest.mark.asyncio
@pytest.mark.usefixtures('load_scheduled_messages')
async def test_scheduled_messages(console_parser, stomp_connection):
    # validate number of scheduled message
    assert await console_parser.scheduled_messages_count() == 10
    # validate messages
    async for m in console_parser.scheduled_messages():
        assert type(m) is ScheduledMessage
        assert type(m.client) is Client
        assert type(m.message_id) is str
        assert type(m.next_scheduled_time) is datetime
        assert type(m.start) is datetime
        assert type(m.delay) is int
        assert type(m.href_delete) is str
