import json
from datetime import datetime
from uuid import UUID

import pytest

from activemq_manager import (
    Broker,
    ActivemqManagerError,
    Connection,
    Queue,
    Message,
    MessageData,
    ScheduledJob,
)


@pytest.mark.asyncio
async def test_broker(broker, activemq_version):
    assert type(broker) is Broker
    assert type(broker.name) is str
    assert (await broker.attribute("BrokerName")) == broker.name
    assert (await broker.attribute("BrokerVersion")) == activemq_version

    _attributes = await broker.attributes()
    assert _attributes.get("BrokerName") == broker.name
    assert _attributes.get("BrokerVersion") == activemq_version


# these tests cannot be used as STOMP does not keep an open connections
@pytest.mark.asyncio
@pytest.mark.usefixtures("stomp_connection")
async def test_connections(broker):
    count = await broker.connection_count()
    assert count == 1
    async for c in broker.connections():
        assert type(c) is Connection
        assert type(c.broker) is Broker
        assert type(c.name) is str
        assert type(c.type) is str
        assert type(c.remote_address) is str
        assert type(c.active) is bool
        assert type(c.slow) is bool


@pytest.mark.usefixtures("load_messages")
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
    assert (await broker.queue("pytest.queue1")).size == 1
    assert (await broker.queue("pytest.queue2")).size == 2
    assert (await broker.queue("pytest.queue3")).size == 3
    assert (await broker.queue("pytest.queue4")).size == 4

    # test Queue.delete() with queue1
    await (await broker.queue("pytest.queue1")).delete()
    with pytest.raises(ActivemqManagerError) as excinfo:
        await broker.queue("pytest.queue1")
    assert "queue not found: pytest.queue1" in str(excinfo.value)

    # test Queue.purge() with queue4
    await (await broker.queue("pytest.queue4")).purge()
    assert (await broker.queue("pytest.queue4")).size == 0


@pytest.mark.asyncio
@pytest.mark.usefixtures("load_messages")
async def test_messages(broker, lorem_ipsum):
    test_queue = await broker.queue("pytest.queue4")

    # validate all messages
    for m in await test_queue.messages():
        assert type(m) is Message
        assert type(m.id) is str
        assert type(m.persistent) is bool
        assert type(m.timestamp) is datetime
        assert type(m.properties) is dict
        assert m.properties.get("test_prop1") == "abcd"
        assert m.properties.get("test_prop2") == "3.14159"
        message_test = await m.text()
        assert type(message_test) is str
        assert message_test == lorem_ipsum

    # test Message.delete()
    messages = iter(await test_queue.messages())
    for _ in range(2):
        msg = next(messages)
        await msg.delete()
    await test_queue.update()
    assert test_queue.size == 2
    assert test_queue.dequeue_count == 2
    assert test_queue.enqueue_count == 4


@pytest.mark.asyncio
@pytest.mark.usefixtures("load_messages")
async def test_message_move(broker, stomp_connection, lorem_ipsum):
    # add the message to the queue and get the message object for it
    stomp_connection.send("pytest.queue_move_source", lorem_ipsum)
    source_queue = await broker.queue("pytest.queue_move_source")
    messages = await source_queue.messages()
    assert len(messages) == 1
    source_message = messages[0]

    # move the message to another queue
    await messages[0].move("pytest.queue_move_target")

    # verify the message was moved
    target_queue = await broker.queue("pytest.queue_move_target")
    messages = await target_queue.messages()
    assert len(messages) == 1
    target_message = messages[0]

    assert source_message.id == target_message.id


def test_parse_byte_array(files_directory):
    # test byte_array1.json
    with open(files_directory.joinpath("byte_array1.json"), "r") as fh:
        _content = json.load(fh)

    assert Message.parse_byte_array(_content["byte_array"]) == _content["value"]

    # test byte_array2.json
    with open(files_directory.joinpath("byte_array2.json"), "r") as fh:
        _content = json.load(fh)

    assert Message.parse_byte_array(_content["byte_array"]) == _content["value"]

    # negative test
    with pytest.raises(ValueError) as excinfo:
        Message.parse_byte_array({"data": "abcd"})
    assert str(excinfo.value) == "data is not a byte array: {'data': 'abcd'}"


@pytest.mark.asyncio
@pytest.mark.usefixtures("load_jobs")
async def test_jobs(broker, stomp_connection):
    # validate number of scheduled message
    assert await broker.job_count() == 10
    # validate messages
    async for j in broker.jobs():
        assert type(j) is ScheduledJob
        assert type(j.broker) is Broker
        assert type(j.id) is str
        assert type(j.next) is datetime
        assert type(j.start) is datetime
        assert type(j.delay) is int
