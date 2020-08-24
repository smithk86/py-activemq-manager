import logging
from datetime import datetime
from uuid import UUID

from .message import Message


logger = logging.getLogger(__name__)


class Queue:
    def __init__(self, broker, name):
        self.broker = broker
        self.name = name
        self._attributes = None

    def __repr__(self):
        return f'<activemq_manager.Queue object name={self.name}>'

    @staticmethod
    async def new(broker, name):
        q = Queue(broker, name)
        return await q.update()

    async def update(self):
        self._attributes = await self.attributes()
        return self

    @property
    def size(self):
        return self._attributes['QueueSize']

    @property
    def enqueue_count(self):
        return self._attributes['EnqueueCount']

    @property
    def dequeue_count(self):
        return self._attributes['DequeueCount']

    @property
    def consumer_count(self):
        return self._attributes['ConsumerCount']

    async def attributes(self):
        return await self.broker.api('read', f'org.apache.activemq:type=Broker,brokerName={self.broker.name},destinationType=Queue,destinationName={self.name}')

    async def purge(self):
        await self.broker.api('exec', f'org.apache.activemq:brokerName={self.broker.name},type=Broker,destinationType=Queue,destinationName={self.name}', operation='purge')

    async def delete(self):
        await self.broker.api('exec', f'org.apache.activemq:type=Broker,brokerName={self.broker.name}', operation='removeQueue(java.lang.String)', arguments=[self.name])

    async def messages(self, selector=None):
        if selector:
            message_table = await self.broker.api('exec', f'org.apache.activemq:brokerName={self.broker.name},type=Broker,destinationType=Queue,destinationName={self.name}', operation='browseAsTable(java.lang.String)', arguments=[selector])
        else:
            message_table = await self.broker.api('exec', f'org.apache.activemq:brokerName={self.broker.name},type=Broker,destinationType=Queue,destinationName={self.name}', operation='browseAsTable()', arguments=[])

        # check and potentially warn if the number of messages returned is less than the total queue size
        await self.update()
        if self.size > len(message_table):
            logger.warning(f'queue size is greater than the returned number of messages [qsize={self.size}, message={len(message_table)}]; use a selector to reduce the total number of messages')

        return [Message(queue=self, id=id, data=data) for id, data in message_table.items()]
