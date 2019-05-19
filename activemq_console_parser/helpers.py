import asyncio
from datetime import datetime

from asyncio_pool import AioPool


def activemq_stamp_datetime(timestamp):
    if len(timestamp) != 19 and len(timestamp) != 24 and len(timestamp) != 27:
        raise ValueError('activemq timestamps are either 20, 24, or 27 characters: got {} ({})'.format(len(timestamp), timestamp))

    microsecond = int(timestamp[20:23])*1000 if len(timestamp) == 26 else 0

    return datetime(
        year=int(timestamp[0:4]),
        month=int(timestamp[5:7]),
        day=int(timestamp[8:10]),
        hour=int(timestamp[11:13]),
        minute=int(timestamp[14:16]),
        second=int(timestamp[17:19]),
        microsecond=microsecond
    )


async def yield_from_pool(aiter_, func, workers=10):
    loop = asyncio.get_running_loop()
    pool = AioPool(workers)
    queues = asyncio.Queue()

    async def add_to_queue(x):
        q = await func(x)
        await queues.put(q)

    async def spawner():
        async for x in aiter_:
            await pool.spawn(add_to_queue(x))
        await pool.join()
        await queues.put(StopAsyncIteration)

    task = loop.create_task(spawner())
    while True:
        val = await queues.get()
        if val is StopAsyncIteration:
            break
        else:
            yield val
    await task
