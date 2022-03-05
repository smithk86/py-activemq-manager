from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .helpers import activemq_stamp_datetime


if TYPE_CHECKING:
    from datetime import datetime
    from typing import Any, Dict
    from .broker import Broker
    from .client import Client


logger = logging.getLogger(__name__)


class ScheduledJob:
    def __init__(self, broker: Broker, id_: str, data: Dict[str, Any]):
        self.broker = broker
        self.id = id_
        self._data = data

    def __repr__(self):
        return f"<activemq_manager.ScheduledJobs object id={self.id}>"

    @property
    def _client(self) -> Client:
        return self.broker._client

    @property
    def start(self) -> datetime:
        return activemq_stamp_datetime(self._data["start"])

    @property
    def next(self) -> datetime:
        return activemq_stamp_datetime(self._data["next"])

    @property
    def delay(self) -> int:
        return self._data["delay"]

    async def delete(self) -> None:
        logger.info(f"delete scheduled message: {self.id} [start={self.start}]")
        await self._client.request(
            "exec",
            f"org.apache.activemq:type=Broker,brokerName={self.broker.name},service=JobScheduler,name=JMS",
            operation="removeJob(java.lang.String)",
            arguments=[self.id],
        )
