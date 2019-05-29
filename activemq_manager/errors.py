class BrokerError(Exception):
    pass


class HttpError(BrokerError):
    pass


class ApiError(HttpError):
    def __init__(self, response):
        self.request = response.get('request')
        self.error = response.get('error')
        self.error_type = response.get('error_type')
        self.status = response.get('status')
        super(ApiError, self).__init__(self.error)
