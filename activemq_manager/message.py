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
        self._properties = properties
        self._content = content

    def __repr__(self):
        return f'<activemq_manager.Message object id={self.message_id}>'

    async def attribute(self, attribute_):
            return await self.broker.api('read', f'org.apache.activemq:type=Broker,brokerName={self.broker.name},destinationType=Queue,destinationName={self.name}', attribute=attribute_)

    @property
    def properties(self):
        props = dict()
        for key, value in self._properties.items():
            if Message.is_byte_array(value):
                props[key] = Message.parse_byte_array(value)
            else:
                props[key] = value
        return props

    @property
    def content(self):
        if self._content is None:
            return None
        return Message.parse_byte_array(self._content)

    async def delete(self):
        logger.info(f'delete message from {self.queue.name}: {self.message_id}')
        await self.queue.broker.api('exec', f'org.apache.activemq:brokerName={self.queue.broker.name},type=Broker,destinationType=Queue,destinationName={self.queue.name}', operation='removeMessage(java.lang.String)', arguments=[self.message_id])

    @staticmethod
    def parse_byte_array(data):
        if not Message.is_byte_array(data):
            raise ValueError(f'data is not a byte array: {data}')
        first_index = data['offset']
        final_index = data['offset'] + data['length']
        trimmed_data = data['data'][first_index:final_index]
        value = ''
        for c in trimmed_data:
            value += chr(c)
        return value

    @staticmethod
    def is_byte_array(data):
        try:
            assert type(data) is dict
            assert type(data['offset']) is int
            assert type(data['length']) is int
            assert type(data['data']) is list
            return True
        except (AssertionError, KeyError):
            return False
