from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import httpx
from asyncio_pool import AioPool

from .connection import Connection
from .errors import ActivemqManagerError
from .helpers import parse_object_name
from .job import ScheduledJob
from .queue import Queue


if TYPE_CHECKING:
    from typing import Any, AsyncGenerator, Dict, Type, Optional
    from .client import Client


logger = logging.getLogger(__name__)


class Broker:
    dtformat: str = '%Y-%m-%d %H:%M:%S'
    _connection_class: Type[Connection] = Connection
    _queue_class: Type[Queue] = Queue

    def __init__(self, client: Client, name: str = 'localhost', workers: int = 10) -> None:
        self._client = client
        self.name = name
        self._pool: AioPool = AioPool(workers)

    def __repr__(self) -> str:
        return f'<activemq_manager.Client object endpoint={self._client.endpoint}>'

    async def attributes(self) -> Dict[str, Any]:
        return await self._client.dict_request('read', f'org.apache.activemq:type=Broker,brokerName={self.name}')

    async def attribute(self, attribute_: str) -> Any:
        return await self._client.request(
            'read',
            f'org.apache.activemq:type=Broker,brokerName={self.name}',
            attribute=attribute_
        )

    async def queues(self) -> AsyncGenerator:
        async def _worker(queue_name) -> Queue:
            return await self._queue_class.new(self, queue_name)

        _queue_names = list()
        for object_name in await self._client.list_request('search', f'org.apache.activemq:type=Broker,brokerName={self.name},destinationType=Queue,destinationName=*'):
            queue_name = parse_object_name(object_name).get('destinationName')
            _queue_names.append(queue_name)

        async with self._pool:
            async for _queue in self._pool.itermap(_worker, _queue_names):
                yield _queue

    async def queue(self, name):
        queue_objects = await self._client.list_request('search', f'org.apache.activemq:type=Broker,brokerName={self.name},destinationType=Queue,destinationName={name}')
        if len(queue_objects) == 1:
            queue_name = parse_object_name(queue_objects[0]).get('destinationName')
            return await self._queue_class.new(self, queue_name)
        else:
            raise ActivemqManagerError(f'queue not found: {name}')

    async def _jobs(
        self,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None
    ) -> Dict[str, Any]:
        if not start:
            start = datetime.now()
        if not end:
            end = start + timedelta(weeks=52)
        return await self._client.dict_request(
            'exec',
            f'org.apache.activemq:type=Broker,brokerName={self.name},service=JobScheduler,name=JMS',
            operation='getAllJobs(java.lang.String,java.lang.String)',
            arguments=[
                start.strftime(Broker.dtformat),
                end.strftime(Broker.dtformat)
            ]
        )

    async def job_count(self, start: Optional[datetime] = None, end: Optional[datetime] = None) -> int:
        count = 0
        for _ in (await self._jobs(start, end)).keys():
            count += 1
        return count

    async def jobs(self, start: Optional[datetime] = None, end: Optional[datetime] = None) -> AsyncGenerator:
        for data in (await self._jobs(start, end)).values():
            yield ScheduledJob(self, data['jobId'], data)

    async def _connections(self) -> AsyncGenerator:
        for connection_type in (await self.attribute('TransportConnectors')).keys():
            for object_name in await self._client.list_request('search', f'org.apache.activemq:type=Broker,brokerName={self.name},connector=clientConnectors,connectorName={connection_type},connectionViewType=remoteAddress,connectionName=*'):
                yield connection_type, object_name

    async def connection_count(self) -> int:
        count = 0
        async for _ in self._connections():
            count += 1
        return count

    async def connections(self, update_attributes: bool = True) -> AsyncGenerator:
        async def _worker(connection) -> Connection:
            return await connection.update()

        _connections = list()
        async for connection_type, object_name in self._connections():
            _parsed_object_name = parse_object_name(object_name)
            if 'connectionName' in _parsed_object_name:
                _connections.append(self._connection_class(
                    self,
                    _parsed_object_name['connectionName'],
                    connection_type
                ))
            else:
                raise ActivemqManagerError(f'connectionName property not found in {_parsed_object_name}')

        if update_attributes is True:
            async with self._pool:
                async for _connection in self._pool.itermap(_worker, _connections):
                    yield _connection
        else:
            for _connection in _connections:
                yield _connection
