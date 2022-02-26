from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from .errors import ActivemqManagerError
from .message import Message


if TYPE_CHECKING:
    from datetime import datetime
    from typing import Any, Dict, List, Optional, Type
    from .broker import Broker
    from .client import Client


logger = logging.getLogger(__name__)


class Queue:
    def __init__(self, broker, name) -> None:
        self.broker = broker
        self.name = name
        self._attributes: Dict[str, Any] = dict()

    def __repr__(self) -> str:
        return f'<activemq_manager.Queue object name={self.name}>'

    @property
    def _client(self) -> Client:
        return self.broker._client

    @staticmethod
    async def new(broker, name) -> Queue:
        q = Queue(broker, name)
        return await q.update()

    async def update(self) -> Queue:
        self._attributes = await self.attributes()
        return self

    def _attribute(self, name: str, expected_type: Type) -> Any:
        if not self._attributes:
            ActivemqManagerError('attributes have not been cached')
        elif name not in self._attributes:
            ActivemqManagerError(f'attribute name does not exist: {name}')
        elif not isinstance(self._attributes[name], expected_type):
            ActivemqManagerError(f'attribute type is incorrect: {name}')

        return self._attributes[name]

    @property
    def size(self) -> int:
        return self._attribute('QueueSize', int)

    @property
    def enqueue_count(self) -> int:
        return self._attribute('EnqueueCount', int)

    @property
    def dequeue_count(self) -> int:
        return self._attribute('DequeueCount', int)

    @property
    def consumer_count(self) -> int:
        return self._attribute('ConsumerCount', int)

    async def attributes(self, attribute=None) -> Dict[str, Any]:
        return await self._client.dict_request('read', f'org.apache.activemq:type=Broker,brokerName={self.broker.name},destinationType=Queue,destinationName={self.name}', attribute=attribute)

    async def purge(self) -> None:
        await self._client.request('exec', f'org.apache.activemq:brokerName={self.broker.name},type=Broker,destinationType=Queue,destinationName={self.name}', operation='purge')

    async def delete(self) -> None:
        await self._client.request('exec', f'org.apache.activemq:type=Broker,brokerName={self.broker.name}', operation='removeQueue(java.lang.String)', arguments=[self.name])

    async def messages(self, selector=None) -> List[Message]:
        if selector:
            message_table = await self._client.dict_request('exec', f'org.apache.activemq:brokerName={self.broker.name},type=Broker,destinationType=Queue,destinationName={self.name}', operation='browseAsTable(java.lang.String)', arguments=[selector])
        else:
            message_table = await self._client.dict_request('exec', f'org.apache.activemq:brokerName={self.broker.name},type=Broker,destinationType=Queue,destinationName={self.name}', operation='browseAsTable()', arguments=[])

        # check and potentially warn if the number of messages returned is less than the total queue size
        await self.update()
        if self.size > len(message_table):
            logger.warning(f'queue size is greater than the returned number of messages [qsize={self.size}, message={len(message_table)}]; use a selector to reduce the total number of messages')

        return [Message(queue=self, id_=id_, attributes=attributes) for id_, attributes in message_table.items()]
