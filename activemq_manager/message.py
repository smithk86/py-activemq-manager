import logging
from collections import namedtuple, OrderedDict

from .errors import BrokerError


logger = logging.getLogger(__name__)
MessageData = namedtuple('MessageData', ['header', 'properties', 'message'])


class Message(object):
    def __init__(self, queue, message_id, persistence, timestamp, properties, content):
        self.queue = queue
        self.message_id = message_id
        self.persistence = persistence
        self.timestamp = timestamp
        self.properties = properties
        self._content = content

    def __repr__(self):
        return f'<activemq_manager.Message object id={self.message_id}>'

    async def attribute(self, attribute_):
            return await self.broker.api('read', f'org.apache.activemq:type=Broker,brokerName={self.broker.name},destinationType=Queue,destinationName={self.name}', attribute=attribute_)

    @property
    def content(self):
        offset = self._content['offset']
        length = self._content['length']
        trimmed_data = self._content['data'][offset:length]
        data = ''
        for c in trimmed_data:
            data += chr(c)
        return data

    async def delete(self):
        logger.info(f'delete message from {self.queue.name}: {self.message_id}')
        await self.queue.broker.api('exec', f'org.apache.activemq:brokerName={self.queue.broker.name},type=Broker,destinationType=Queue,destinationName={self.queue.name}', operation='removeMessage(java.lang.String)', arguments=[self.message_id])
