class InvalidEurostatApiResponse(Exception):
    ERR_MSG = 'Api returned {status_code} instead of expected 200!'

    def __init__(self, status_code):
        super(InvalidEurostatApiResponse, self).__init__(self.ERR_MSG.format(status_code=status_code))
