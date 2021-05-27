import logging
from collections import namedtuple, OrderedDict

import dateparser

from .errors import BrokerError


logger = logging.getLogger(__name__)
MessageData = namedtuple('MessageData', ['header', 'properties', 'message'])


class Message(object):
    def __init__(self, queue, id, data):
        self.queue = queue
        self.id = id
        self._data = data

    def __repr__(self):
        return f'<activemq_manager.Message object id={self.id}>'

    @property
    def timestamp(self):
        return dateparser.parse(self._data['JMSTimestamp'])

    @property
    def persistent(self):
        return self._data['JMSDeliveryMode'] == 'PERSISTENT'

    @property
    def properties(self):
        return self._data['StringProperties']

    async def data(self):
        api_response = await self.queue.broker.api('exec', f'org.apache.activemq:brokerName={self.queue.broker.name},type=Broker,destinationType=Queue,destinationName={self.queue.name}', operation='browseMessages(java.lang.String)', arguments=[f"JMSMessageID = '{self.id}'"])
        if len(api_response) == 1:
            return api_response[0]
        else:
            raise BrokerError(f'only one message should have been return [count={len(api_response)}]')

    async def text(self):
        data = await self.data()
        if 'text' in data:
            return data['text']
        elif 'content' in data:
            return Message.parse_byte_array(data['content'])
        else:
            raise BrokerError(f'cannot parse message content from {data}')

    async def delete(self):
        logger.info(f'delete message from {self.queue.name}: {self.id}')
        return await self.queue.broker.api('exec', f'org.apache.activemq:brokerName={self.queue.broker.name},type=Broker,destinationType=Queue,destinationName={self.queue.name}', operation='removeMessage(java.lang.String)', arguments=[self.id])

    async def move(self, target_queue):
        logger.info(f'moving message from {self.queue.name} to {target_queue}: {self.id}')
        return await self.queue.broker.api('exec', f'org.apache.activemq:brokerName={self.queue.broker.name},type=Broker,destinationType=Queue,destinationName={self.queue.name}', operation='moveMessageTo(java.lang.String, java.lang.String)', arguments=[self.id, target_queue])

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
