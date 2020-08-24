import logging

from .helpers import activemq_stamp_datetime


logger = logging.getLogger(__name__)


class ScheduledJob(object):
    def __init__(self, broker, id, data):
        self.broker = broker
        self.id = id
        self._data = data

    def __repr__(self):
        return f'<activemq_manager.ScheduledJobs object id={self.id}>'

    @property
    def start(self):
        return activemq_stamp_datetime(self._data['start'])

    @property
    def next(self):
        return activemq_stamp_datetime(self._data['next'])

    @property
    def delay(self):
        return self._data['delay']

    async def delete(self):
        logger.info(f'delete scheduled message: {self.id} [start={self.start}]')
        await self.broker.api('exec', f'org.apache.activemq:type=Broker,brokerName={self.broker.name},service=JobScheduler,name=JMS', operation='removeJob(java.lang.String)', arguments=[
            self.id
        ])
