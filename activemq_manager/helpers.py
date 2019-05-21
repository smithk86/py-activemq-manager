from datetime import datetime


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
