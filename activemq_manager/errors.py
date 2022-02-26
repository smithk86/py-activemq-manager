import collections.abc


class ExtendedException(Exception, collections.abc.Mapping):
    def __init__(self, message, **data):
        self.data = dict(message=message, **data)
        super().__init__(message)

    def __iter__(self):
        return iter(self.data.keys())

    def __getitem__(self, key):
        return self.data[key]

    def __len__(self):
        return len(self.data)


class ActivemqManagerError(ExtendedException):
    pass
