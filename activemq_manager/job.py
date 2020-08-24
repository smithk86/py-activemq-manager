import logging

from .helpers import activemq_stamp_datetime


logger = logging.getLogger(__name__)


class ScheduledJob(object):
    def __init__(self, broker, job_id, start, next, delay):
        self.broker = broker
        self.job_id = job_id
        self.start = start
        self.next = next
        self.delay = delay

    def __repr__(self):
        return f'<activemq_manager.ScheduledJobs object id={self.job_id}>'

    @staticmethod
    def parse(broker, data):
        return ScheduledJob(
            broker=broker,
            job_id=data.get('jobId'),
            start=activemq_stamp_datetime(data.get('start')),
            next=activemq_stamp_datetime(data.get('next')),
            delay=data.get('delay')
        )

    async def delete(self):
        logger.info(f'delete scheduled message: {self.job_id} [start={self.start}]')
        await self.broker.api('exec', f'org.apache.activemq:type=Broker,brokerName={self.broker.name},service=JobScheduler,name=JMS', operation='removeJob(java.lang.String)', arguments=[
            self.job_id
        ])
