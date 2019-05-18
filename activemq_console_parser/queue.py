import logging
from datetime import datetime
from uuid import UUID

from .message import Message


logger = logging.getLogger(__name__)


class Queue:
    def __init__(self, client, name, messages_pending, messages_enqueued, messages_dequeued, consumers):
        self.client = client
        self.name = name
        self.messages_pending = messages_pending
        self.messages_enqueued = messages_enqueued
        self.messages_dequeued = messages_dequeued
        self.consumers = consumers

    def __repr__(self):
        return f'<activemq_console_parser.queue.Queue object name={self.name}>'

    def asdict(self):
        return {
            'name': self.name,
            'messages_pending': self.messages_pending,
            'messages_enqueued': self.messages_enqueued,
            'messages_dequeued': self.messages_dequeued,
            'consumers': self.consumers
        }

    async def purge(self):
        await self.client.api('exec', f'org.apache.activemq:brokerName={self.client.broker_name},type=Broker,destinationType=Queue,destinationName={self.name}', operation='purge')

    async def delete(self):
        await self.client.api('exec', f'org.apache.activemq:type=Broker,brokerName={self.client.broker_name}', operation='removeQueue(java.lang.String)', arguments=[self.name])

    async def messages(self):
        for m in await self.client.api('exec', f'org.apache.activemq:brokerName={self.client.broker_name},type=Broker,destinationType=Queue,destinationName={self.name}', operation='browseMessages()', arguments=[]):
            yield Message(
                queue=self,
                message_id=m.get('jMSMessageID'),
                persistence=m.get('persistent'),
                timestamp=datetime.utcfromtimestamp(m.get('jMSTimestamp') / 1000)
            )
