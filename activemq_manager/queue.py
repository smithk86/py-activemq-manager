import logging
from datetime import datetime
from uuid import UUID

from .message import Message


logger = logging.getLogger(__name__)


class Queue:
    def __init__(self, broker, name):
        self.broker = broker
        self.name = name
        self.size = None
        self.enqueue_count = None
        self.dequeue_count = None
        self.consumer_count = None

    @staticmethod
    async def new(broker, name):
        q = Queue(broker, name)
        await q.update()
        return q

    def __repr__(self):
        return f'<activemq_manager.Queue object name={self.name}>'

    async def update(self):
        data = await self.attribute([
            'QueueSize',
            'EnqueueCount',
            'DequeueCount',
            'ConsumerCount'
        ])
        self.size = data.get('QueueSize')
        self.enqueue_count = data.get('EnqueueCount')
        self.dequeue_count = data.get('DequeueCount')
        self.consumer_count = data.get('ConsumerCount')

    def asdict(self):
        return {
            'name': self.name,
            'size': self.size,
            'enqueue_count': self.enqueue_count,
            'dequeue_count': self.dequeue_count,
            'consumer_count': self.consumer_count
        }

    async def attribute(self, attribute_):
        return await self.broker.api('read', f'org.apache.activemq:type=Broker,brokerName={self.broker.name},destinationType=Queue,destinationName={self.name}', attribute=attribute_)

    async def purge(self):
        await self.broker.api('exec', f'org.apache.activemq:brokerName={self.broker.name},type=Broker,destinationType=Queue,destinationName={self.name}', operation='purge')

    async def delete(self):
        await self.broker.api('exec', f'org.apache.activemq:type=Broker,brokerName={self.broker.name}', operation='removeQueue(java.lang.String)', arguments=[self.name])

    async def messages(self):
        message_table = await self.broker.api('exec', f'org.apache.activemq:brokerName={self.broker.name},type=Broker,destinationType=Queue,destinationName={self.name}', operation='browseAsTable()', arguments=[])
        return [Message(queue=self, data=data) for data in message_table.values()]
