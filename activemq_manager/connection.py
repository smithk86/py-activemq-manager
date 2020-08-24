import logging
from datetime import datetime
from uuid import UUID


logger = logging.getLogger(__name__)


class Connection:
    def __init__(self, broker, name, type):
        self.broker = broker
        self.name = name
        self.type = type
        self.client_id = None
        self.remote_address = None
        self.active = None
        self.slow = None

    @staticmethod
    async def new(broker, name, type):
        c = Connection(broker, name, type)
        await c.update()
        return c

    def __repr__(self):
        return f'<activemq_manager.Connection object name={self.name}>'

    async def update(self):
        data = await self.attribute([
            'ClientId',
            'RemoteAddress',
            'Active',
            'Slow'
        ])
        self.client_id = data.get('ClientId')
        self.remote_address = data.get('RemoteAddress')
        self.active = data.get('Active')
        self.slow = data.get('Slow')

    async def attribute(self, attribute_):
        return await self.broker.api('read', f'org.apache.activemq:type=Broker,brokerName={self.broker.name},connector=clientConnectors,connectorName={self.type},connectionViewType=remoteAddress,connectionName={self.name}', attribute=attribute_)
