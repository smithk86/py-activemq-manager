from __future__ import annotations

import logging
from datetime import datetime
from uuid import UUID
from typing import TYPE_CHECKING

from .errors import ActivemqManagerError


if TYPE_CHECKING:
    from typing import Any, Dict, Optional, Type
    from .broker import Broker


logger = logging.getLogger(__name__)


class Connection:
    # exclude 'Consumers' and 'Producers' as these tend to contain a large amount of data
    default_attribute = [
        'Active',
        'ActiveTransactionCount',
        'Blocked',
        'ClientId',
        'Connected',
        'DispatchQueueSize',
        'OldestActiveTransactionDuration',
        'RemoteAddress',
        'Slow',
        'UserName'
    ]

    def __init__(self, broker: Broker, name: str, type_: str) -> None:
        self.broker = broker
        self.name = name
        self.type = type_
        self._attributes: Dict[str, Any] = {}

    def __repr__(self) -> str:
        return f'<activemq_manager.Connection object name={self.name}>'

    @property
    def _client(self):
        return self.broker._client

    @staticmethod
    async def new(broker: Broker, name: str, type_: str) -> Connection:
        conn = Connection(broker, name, type_)
        return await conn.update()

    def _attribute(self, name: str, expected_type: Type) -> Any:
        if not self._attributes:
            ActivemqManagerError('attributes have not been cached')
        elif name not in self._attributes:
            ActivemqManagerError(f'attribute name does not exist: {name}')
        elif not isinstance(self._attributes[name], expected_type):
            ActivemqManagerError(f'attribute type is incorrect: {name}')

        return self._attributes[name]

    @property
    def client_id(self) -> str:
        return self._attribute('ClientId', str)

    @property
    def remote_address(self) -> str:
        return self._attribute('RemoteAddress', str)

    @property
    def active(self) -> bool:
        return self._attribute('Active', bool)

    @property
    def slow(self) -> bool:
        return self._attribute('Slow', bool)

    async def update(self) -> Connection:
        self._attributes = await self.attributes()
        return self

    async def attributes(self, attribute=None) -> Dict[str, Any]:
        if attribute is None:
            attribute = self.default_attribute

        return await self.broker._client.dict_request('read', f'org.apache.activemq:type=Broker,brokerName={self.broker.name},connector=clientConnectors,connectorName={self.type},connectionViewType=remoteAddress,connectionName={self.name}', attribute=attribute)
