import logging
from collections import namedtuple
from datetime import datetime
from uuid import UUID


from .message import Message


logger = logging.getLogger(__name__)
QueueData = namedtuple('QueueData', ['size', 'enqueue_count', 'dequeue_count', 'consumer_count'])


class Queue:
    def __init__(self, broker, name):
        self.broker = broker
        self.name = name

    def __repr__(self):
        return f'<activemq_manager.Queue object name={self.name}>'

    async def attribute(self, attribute_):
            return await self.broker.api('read', f'org.apache.activemq:type=Broker,brokerName={self.broker.name},destinationType=Queue,destinationName={self.name}', attribute=attribute_)

    async def data(self):
        data = await self.attribute([
            'QueueSize',
            'EnqueueCount',
            'DequeueCount',
            'ConsumerCount'
        ])
        return QueueData(
            size=data.get('QueueSize'),
            enqueue_count=data.get('EnqueueCount'),
            dequeue_count=data.get('DequeueCount'),
            consumer_count=data.get('ConsumerCount')
        )

    async def purge(self):
        await self.broker.api('exec', f'org.apache.activemq:brokerName={self.broker.name},type=Broker,destinationType=Queue,destinationName={self.name}', operation='purge')

    async def delete(self):
        await self.broker.api('exec', f'org.apache.activemq:type=Broker,brokerName={self.broker.name}', operation='removeQueue(java.lang.String)', arguments=[self.name])

    async def messages(self):
        for m in await self.broker.api('exec', f'org.apache.activemq:brokerName={self.broker.name},type=Broker,destinationType=Queue,destinationName={self.name}', operation='browseMessages()', arguments=[]):
            yield Message(
                queue=self,
                message_id=m.get('jMSMessageID'),
                persistence=m.get('persistent'),
                timestamp=datetime.utcfromtimestamp(m.get('jMSTimestamp') / 1000),
                properties=m.get('properties'),
                content=m.get('content')
            )
