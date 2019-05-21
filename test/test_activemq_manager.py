from datetime import datetime
from uuid import UUID

import pytest

from activemq_manager import Broker, BrokerError, Connection, Queue, QueueData, Message, MessageData, ScheduledJob


async def alist(aiter_):
    list_ = list()
    async for x in aiter_:
        list_.append(x)
    return list_


# these tests cannot be used as STOMP does not keep an open connections
# @pytest.mark.asyncio
# @pytest.mark.usefixtures('load_messages')
# async def test_connections(broker):
#     connections = await alist(broker.connections())
#     assert len(connections) > 0
#     for c in connections:
#         assert type(c) is Connection
#         assert type(c.broker) is Broker
#         assert isinstance(c._asdict(), dict)
#         assert type(c.id) is str
#         assert type(c.remote_address) is str
#         assert type(c.active) is bool
#         assert type(c.slow) is bool


@pytest.mark.asyncio
async def test_broker(broker, activemq_version):
    assert type(broker) is Broker
    assert type(broker.name) is str
    assert await broker.attribute('BrokerName') == broker.name
    assert await broker.attribute('BrokerVersion') == activemq_version


@pytest.mark.usefixtures('load_messages')
@pytest.mark.asyncio
async def test_queues(broker):
    async for q in broker.queues():
        assert type(q) is Queue
        assert type(q.broker) is Broker
        assert type(q.name) is str
        data = await q.data()
        assert type(data) is QueueData
        assert type(data.size) is int
        assert type(data.enqueue_count) is int
        assert type(data.dequeue_count) is int
        assert type(data.consumer_count) is int

    # assert the number of mesages in each queue
    assert (await (await broker.queue('pytest.queue1')).data()).size == 1
    assert (await (await broker.queue('pytest.queue2')).data()).size == 2
    assert (await (await broker.queue('pytest.queue3')).data()).size == 3
    assert (await (await broker.queue('pytest.queue4')).data()).size == 4

    # test Queue.delete() with queue1
    await (await broker.queue('pytest.queue1')).delete()
    with pytest.raises(BrokerError) as excinfo:
        await broker.queue('pytest.queue1')
    assert 'queue not found: pytest.queue1' in str(excinfo.value)

    # test Queue.purge() with queue4
    await (await broker.queue('pytest.queue4')).purge()
    assert (await (await broker.queue('pytest.queue4')).data()).size == 0


@pytest.mark.asyncio
@pytest.mark.usefixtures('load_messages')
async def test_messages(broker):
    test_queue = await broker.queue('pytest.queue4')

    # validate all messages
    async for m in test_queue.messages():
        assert type(m) is Message
        assert type(m.message_id) is str
        assert type(m.persistence) is bool
        assert type(m.timestamp) is datetime
        assert type(m.properties) is dict
        assert m.properties.get('test_prop') == 'abcd'
        assert type(m.content) is str
        UUID(m.content)  # ensure the message is a uuid

    # test Message.delete()
    messages = test_queue.messages()
    for _ in range(2):
        msg = await messages.__anext__()
        await msg.delete()
    test_queue_data = await test_queue.data()
    assert test_queue_data.size == 2
    assert test_queue_data.dequeue_count == 2
    assert test_queue_data.enqueue_count == 4


@pytest.mark.asyncio
@pytest.mark.usefixtures('load_jobs')
async def test_jobs(broker, stomp_connection):
    # validate number of scheduled message
    assert await broker.job_count() == 10
    # validate messages
    async for j in broker.jobs():
        assert type(j) is ScheduledJob
        assert type(j.broker) is Broker
        assert type(j.job_id) is str
        assert type(j.next) is datetime
        assert type(j.start) is datetime
        assert type(j.delay) is int
