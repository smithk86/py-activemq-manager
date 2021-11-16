import sys


__VERSION__ = '0.0.1-dev'
__DATE__ = '2021-11-16'
__MIN_PYTHON__ = (3, 7)


if sys.version_info < __MIN_PYTHON__:
    sys.exit('python {}.{} or later is required'.format(*__MIN_PYTHON__))


from .broker import Broker
from .client import Client
from .connection import Connection
from .errors import ActivemqManagerError
from .job import ScheduledJob
from .queue import Queue
from .message import Message, MessageData
