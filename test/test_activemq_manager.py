from datetime import datetime
from uuid import UUID

import pytest

from activemq_manager import Broker, BrokerError, Connection, Queue, Message, MessageData, ScheduledJob


# these tests cannot be used as STOMP does not keep an open connections
@pytest.mark.asyncio
@pytest.mark.usefixtures('stomp_connection')
async def test_connections(broker):
    count = await broker.connection_count()
    assert count == 1
    async for c in broker.connections():
        assert type(c) is Connection
        assert type(c.broker) is Broker
        assert isinstance(c.asdict(), dict)
        assert type(c.name) is str
        assert type(c.type) is str
        assert type(c.remote_address) is str
        assert type(c.active) is bool
        assert type(c.slow) is bool


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
        assert type(q.size) is int
        assert type(q.enqueue_count) is int
        assert type(q.dequeue_count) is int
        assert type(q.consumer_count) is int

    # assert the number of mesages in each queue
    assert (await broker.queue('pytest.queue1')).size == 1
    assert (await broker.queue('pytest.queue2')).size == 2
    assert (await broker.queue('pytest.queue3')).size == 3
    assert (await broker.queue('pytest.queue4')).size == 4

    # test Queue.delete() with queue1
    await (await broker.queue('pytest.queue1')).delete()
    with pytest.raises(BrokerError) as excinfo:
        await broker.queue('pytest.queue1')
    assert 'queue not found: pytest.queue1' in str(excinfo.value)

    # test Queue.purge() with queue4
    await (await broker.queue('pytest.queue4')).purge()
    assert (await broker.queue('pytest.queue4')).size == 0


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
    await test_queue.update()
    assert test_queue.size == 2
    assert test_queue.dequeue_count == 2
    assert test_queue.enqueue_count == 4


def test_parse_byte_array():
    value = 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Praesent consectetur dictum leo, et euismod sem fermentum in. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Integer rhoncus quam vitae elit ullamcorper ultricies. Mauris a elit metus. Quisque in purus non ipsum vestibulum suscipit. Sed mattis ornare ante, non rutrum nisl tristique non. Ut finibus mattis arcu sit amet convallis. Ut rhoncus augue tortor, at commodo libero consequat eu. Aenean ligula orci, malesuada non tellus nec, dictum bibendum lacus. Fusce in nunc lacinia, condimentum dolor sed, mollis turpis.'
    value_byte_array = {
        'offset': 4,
        'length': 619,
        'data': [0, 0, 0, 0, 76, 111, 114, 101, 109, 32, 105, 112, 115, 117, 109, 32, 100, 111, 108, 111, 114, 32, 115, 105, 116, 32, 97, 109, 101, 116, 44, 32, 99, 111, 110, 115, 101, 99, 116, 101, 116, 117, 114, 32, 97, 100, 105, 112, 105, 115, 99, 105, 110, 103, 32, 101, 108, 105, 116, 46, 32, 80, 114, 97, 101, 115, 101, 110, 116, 32, 99, 111, 110, 115, 101, 99, 116, 101, 116, 117, 114, 32, 100, 105, 99, 116, 117, 109, 32, 108, 101, 111, 44, 32, 101, 116, 32, 101, 117, 105, 115, 109, 111, 100, 32, 115, 101, 109, 32, 102, 101, 114, 109, 101, 110, 116, 117, 109, 32, 105, 110, 46, 32, 67, 108, 97, 115, 115, 32, 97, 112, 116, 101, 110, 116, 32, 116, 97, 99, 105, 116, 105, 32, 115, 111, 99, 105, 111, 115, 113, 117, 32, 97, 100, 32, 108, 105, 116, 111, 114, 97, 32, 116, 111, 114, 113, 117, 101, 110, 116, 32, 112, 101, 114, 32, 99, 111, 110, 117, 98, 105, 97, 32, 110, 111, 115, 116, 114, 97, 44, 32, 112, 101, 114, 32, 105, 110, 99, 101, 112, 116, 111, 115, 32, 104, 105, 109, 101, 110, 97, 101, 111, 115, 46, 32, 73, 110, 116, 101, 103, 101, 114, 32, 114, 104, 111, 110, 99, 117, 115, 32, 113, 117, 97, 109, 32, 118, 105, 116, 97, 101, 32, 101, 108, 105, 116, 32, 117, 108, 108, 97, 109, 99, 111, 114, 112, 101, 114, 32, 117, 108, 116, 114, 105, 99, 105, 101, 115, 46, 32, 77, 97, 117, 114, 105, 115, 32, 97, 32, 101, 108, 105, 116, 32, 109, 101, 116, 117, 115, 46, 32, 81, 117, 105, 115, 113, 117, 101, 32, 105, 110, 32, 112, 117, 114, 117, 115, 32, 110, 111, 110, 32, 105, 112, 115, 117, 109, 32, 118, 101, 115, 116, 105, 98, 117, 108, 117, 109, 32, 115, 117, 115, 99, 105, 112, 105, 116, 46, 32, 83, 101, 100, 32, 109, 97, 116, 116, 105, 115, 32, 111, 114, 110, 97, 114, 101, 32, 97, 110, 116, 101, 44, 32, 110, 111, 110, 32, 114, 117, 116, 114, 117, 109, 32, 110, 105, 115, 108, 32, 116, 114, 105, 115, 116, 105, 113, 117, 101, 32, 110, 111, 110, 46, 32, 85, 116, 32, 102, 105, 110, 105, 98, 117, 115, 32, 109, 97, 116, 116, 105, 115, 32, 97, 114, 99, 117, 32, 115, 105, 116, 32, 97, 109, 101, 116, 32, 99, 111, 110, 118, 97, 108, 108, 105, 115, 46, 32, 85, 116, 32, 114, 104, 111, 110, 99, 117, 115, 32, 97, 117, 103, 117, 101, 32, 116, 111, 114, 116, 111, 114, 44, 32, 97, 116, 32, 99, 111, 109, 109, 111, 100, 111, 32, 108, 105, 98, 101, 114, 111, 32, 99, 111, 110, 115, 101, 113, 117, 97, 116, 32, 101, 117, 46, 32, 65, 101, 110, 101, 97, 110, 32, 108, 105, 103, 117, 108, 97, 32, 111, 114, 99, 105, 44, 32, 109, 97, 108, 101, 115, 117, 97, 100, 97, 32, 110, 111, 110, 32, 116, 101, 108, 108, 117, 115, 32, 110, 101, 99, 44, 32, 100, 105, 99, 116, 117, 109, 32, 98, 105, 98, 101, 110, 100, 117, 109, 32, 108, 97, 99, 117, 115, 46, 32, 70, 117, 115, 99, 101, 32, 105, 110, 32, 110, 117, 110, 99, 32, 108, 97, 99, 105, 110, 105, 97, 44, 32, 99, 111, 110, 100, 105, 109, 101, 110, 116, 117, 109, 32, 100, 111, 108, 111, 114, 32, 115, 101, 100, 44, 32, 109, 111, 108, 108, 105, 115, 32, 116, 117, 114, 112, 105, 115, 46, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    }
    # positive test
    assert Message.parse_byte_array(value_byte_array) == value
    # positive test
    value_byte_array = {
        'offset': 0,
        'length': 619,
        'data': [76, 111, 114, 101, 109, 32, 105, 112, 115, 117, 109, 32, 100, 111, 108, 111, 114, 32, 115, 105, 116, 32, 97, 109, 101, 116, 44, 32, 99, 111, 110, 115, 101, 99, 116, 101, 116, 117, 114, 32, 97, 100, 105, 112, 105, 115, 99, 105, 110, 103, 32, 101, 108, 105, 116, 46, 32, 80, 114, 97, 101, 115, 101, 110, 116, 32, 99, 111, 110, 115, 101, 99, 116, 101, 116, 117, 114, 32, 100, 105, 99, 116, 117, 109, 32, 108, 101, 111, 44, 32, 101, 116, 32, 101, 117, 105, 115, 109, 111, 100, 32, 115, 101, 109, 32, 102, 101, 114, 109, 101, 110, 116, 117, 109, 32, 105, 110, 46, 32, 67, 108, 97, 115, 115, 32, 97, 112, 116, 101, 110, 116, 32, 116, 97, 99, 105, 116, 105, 32, 115, 111, 99, 105, 111, 115, 113, 117, 32, 97, 100, 32, 108, 105, 116, 111, 114, 97, 32, 116, 111, 114, 113, 117, 101, 110, 116, 32, 112, 101, 114, 32, 99, 111, 110, 117, 98, 105, 97, 32, 110, 111, 115, 116, 114, 97, 44, 32, 112, 101, 114, 32, 105, 110, 99, 101, 112, 116, 111, 115, 32, 104, 105, 109, 101, 110, 97, 101, 111, 115, 46, 32, 73, 110, 116, 101, 103, 101, 114, 32, 114, 104, 111, 110, 99, 117, 115, 32, 113, 117, 97, 109, 32, 118, 105, 116, 97, 101, 32, 101, 108, 105, 116, 32, 117, 108, 108, 97, 109, 99, 111, 114, 112, 101, 114, 32, 117, 108, 116, 114, 105, 99, 105, 101, 115, 46, 32, 77, 97, 117, 114, 105, 115, 32, 97, 32, 101, 108, 105, 116, 32, 109, 101, 116, 117, 115, 46, 32, 81, 117, 105, 115, 113, 117, 101, 32, 105, 110, 32, 112, 117, 114, 117, 115, 32, 110, 111, 110, 32, 105, 112, 115, 117, 109, 32, 118, 101, 115, 116, 105, 98, 117, 108, 117, 109, 32, 115, 117, 115, 99, 105, 112, 105, 116, 46, 32, 83, 101, 100, 32, 109, 97, 116, 116, 105, 115, 32, 111, 114, 110, 97, 114, 101, 32, 97, 110, 116, 101, 44, 32, 110, 111, 110, 32, 114, 117, 116, 114, 117, 109, 32, 110, 105, 115, 108, 32, 116, 114, 105, 115, 116, 105, 113, 117, 101, 32, 110, 111, 110, 46, 32, 85, 116, 32, 102, 105, 110, 105, 98, 117, 115, 32, 109, 97, 116, 116, 105, 115, 32, 97, 114, 99, 117, 32, 115, 105, 116, 32, 97, 109, 101, 116, 32, 99, 111, 110, 118, 97, 108, 108, 105, 115, 46, 32, 85, 116, 32, 114, 104, 111, 110, 99, 117, 115, 32, 97, 117, 103, 117, 101, 32, 116, 111, 114, 116, 111, 114, 44, 32, 97, 116, 32, 99, 111, 109, 109, 111, 100, 111, 32, 108, 105, 98, 101, 114, 111, 32, 99, 111, 110, 115, 101, 113, 117, 97, 116, 32, 101, 117, 46, 32, 65, 101, 110, 101, 97, 110, 32, 108, 105, 103, 117, 108, 97, 32, 111, 114, 99, 105, 44, 32, 109, 97, 108, 101, 115, 117, 97, 100, 97, 32, 110, 111, 110, 32, 116, 101, 108, 108, 117, 115, 32, 110, 101, 99, 44, 32, 100, 105, 99, 116, 117, 109, 32, 98, 105, 98, 101, 110, 100, 117, 109, 32, 108, 97, 99, 117, 115, 46, 32, 70, 117, 115, 99, 101, 32, 105, 110, 32, 110, 117, 110, 99, 32, 108, 97, 99, 105, 110, 105, 97, 44, 32, 99, 111, 110, 100, 105, 109, 101, 110, 116, 117, 109, 32, 100, 111, 108, 111, 114, 32, 115, 101, 100, 44, 32, 109, 111, 108, 108, 105, 115, 32, 116, 117, 114, 112, 105, 115, 46, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    }
    # positive test
    assert Message.parse_byte_array(value_byte_array) == value
    # negative test
    with pytest.raises(ValueError) as excinfo:
        Message.parse_byte_array({'data': 'abcd'})
    assert str(excinfo.value) == "data is not a byte array: {'data': 'abcd'}"


@staticmethod
def parse_byte_array(data):
    if not Message.is_byte_array(data):
        raise ValueError(f'data is not a byte array: {data}')
    offset = data['offset']
    length = data['length']
    trimmed_data = data['data'][offset:length]
    value = ''
    for c in trimmed_data:
        value += chr(c)
    return value


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
