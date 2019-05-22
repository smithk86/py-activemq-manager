import logging
from datetime import datetime
from uuid import UUID

from asyncinit import asyncinit


logger = logging.getLogger(__name__)


@asyncinit
class Connection:
    async def __init__(self, broker, name):
        self.broker = broker
        self.name = name
        self.client_id = None
        self.remote_address = None
        self.active = None
        self.slow = None
        await self.update()

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

    def asdict(self):
        return {
            'name': self.name,
            'client_id': self.client_id,
            'remote_address': self.remote_address,
            'active': self.active,
            'slow': self.slow
        }

    async def attribute(self, attribute_):
        return await self.broker.api('read', f'org.apache.activemq:type=Broker,brokerName={self.broker.name},connector=clientConnectors,connectorName=openwire,connectionViewType=remoteAddress,connectionName={self.name}', attribute=attribute_)
