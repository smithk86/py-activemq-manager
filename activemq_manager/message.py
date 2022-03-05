from __future__ import annotations

import logging
from collections import namedtuple, OrderedDict
from typing import TYPE_CHECKING

import dateparser

from .errors import ActivemqManagerError


if TYPE_CHECKING:
    from datetime import datetime
    from typing import Any, Dict, List, Optional
    from .queue import Queue


logger = logging.getLogger(__name__)
MessageData = namedtuple("MessageData", ["header", "properties", "message"])


class Message:
    def __init__(self, queue: Queue, id_: str, attributes: Dict[str, Any]):
        self.queue = queue
        self.id = id_
        self._attributes = attributes

    def __repr__(self) -> str:
        return f"<activemq_manager.Message object id={self.id}>"

    @property
    def _client(self):
        return self.queue._client

    @property
    def timestamp(self) -> Optional[datetime]:
        return dateparser.parse(self._attributes["JMSTimestamp"])

    @property
    def persistent(self) -> bool:
        return self._attributes["JMSDeliveryMode"] == "PERSISTENT"

    @property
    def properties(self) -> Dict[str, str]:
        return self._attributes["StringProperties"]

    async def data(self):
        api_response = await self._client.list_request(
            "exec",
            f"org.apache.activemq:brokerName={self.queue.broker.name},type=Broker,destinationType=Queue,destinationName={self.queue.name}",
            operation="browseMessages(java.lang.String)",
            arguments=[f"JMSMessageID = '{self.id}'"],
        )
        if len(api_response) == 1:
            return api_response[0]
        else:
            raise ActivemqManagerError(
                f"only one message should have been return [count={len(api_response)}]"
            )

    async def text(self) -> str:
        data = await self.data()
        if "text" in data:
            return data["text"]
        elif "content" in data:
            return self.parse_byte_array(data["content"])
        else:
            raise ActivemqManagerError(f"cannot parse message content from {data}")

    async def delete(self) -> None:
        logger.info(f"delete message from {self.queue.name}: {self.id}")
        await self._client.request(
            "exec",
            f"org.apache.activemq:brokerName={self.queue.broker.name},type=Broker,destinationType=Queue,destinationName={self.queue.name}",
            operation="removeMessage(java.lang.String)",
            arguments=[self.id],
        )

    async def retry(self) -> None:
        logger.info(f"retrying message from {self.queue.name}: {self.id}")
        await self._client.request(
            "exec",
            f"org.apache.activemq:brokerName={self.queue.broker.name},type=Broker,destinationType=Queue,destinationName={self.queue.name}",
            operation="retryMessage(java.lang.String)",
            arguments=[self.id],
        )

    async def move(self, target_queue) -> None:
        logger.info(
            f"moving message from {self.queue.name} to {target_queue}: {self.id}"
        )
        await self._client.request(
            "exec",
            f"org.apache.activemq:brokerName={self.queue.broker.name},type=Broker,destinationType=Queue,destinationName={self.queue.name}",
            operation="moveMessageTo(java.lang.String, java.lang.String)",
            arguments=[self.id, target_queue],
        )

    @staticmethod
    def parse_byte_array(data) -> str:
        if not Message.is_byte_array(data):
            raise ValueError(f"data is not a byte array: {data}")
        first_index = data["offset"]
        final_index = data["offset"] + data["length"]
        trimmed_data = data["data"][first_index:final_index]
        value = ""
        for c in trimmed_data:
            value += chr(c)
        return value

    @staticmethod
    def is_byte_array(data: Any) -> bool:
        try:
            assert type(data) is dict
            assert type(data["offset"]) is int
            assert type(data["length"]) is int
            assert type(data["data"]) is list
            return True
        except (AssertionError, KeyError):
            return False
