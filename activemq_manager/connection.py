import logging
from datetime import datetime
from uuid import UUID


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

    def __init__(self, broker, name, type):
        self.broker = broker
        self.name = name
        self.type = type
        self._attributes = None

    def __repr__(self):
        return f'<activemq_manager.Connection object name={self.name}>'

    @staticmethod
    async def new(broker, name, type):
        conn = Connection(broker, name, type)
        return await conn.update()

    @property
    def client_id(self):
        return self._attributes['ClientId']

    @property
    def remote_address(self):
        return self._attributes['RemoteAddress']

    @property
    def active(self):
        return self._attributes['Active']

    @property
    def slow(self):
        return self._attributes['Slow']

    async def update(self):
        self._attributes = await self.attributes()
        return self

    async def attributes(self, attribute=None):
        if attribute is None:
            attribute = Connection.default_attribute
        return await self.broker.api('read', f'org.apache.activemq:type=Broker,brokerName={self.broker.name},connector=clientConnectors,connectorName={self.type},connectionViewType=remoteAddress,connectionName={self.name}', attribute=attribute)
