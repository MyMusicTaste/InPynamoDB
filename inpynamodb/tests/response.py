"""
Mock response
"""
from aiohttp.web_response import Response


class MockResponse(Response):
    """
    A class for mocked responses
    """
    def __init__(self, status_code=None, content='Empty'):
        super(MockResponse, self).__init__(status=status_code, text=content, reason='Test Response')


class HttpBadRequest(MockResponse):
    """
    A response class that returns status 400
    """
    def __init__(self):
        super(HttpBadRequest, self).__init__(status_code=400)


class HttpUnavailable(MockResponse):
    """
    A response that returns status code 502
    """
    def __init__(self):
        super(HttpUnavailable, self).__init__(status_code=502)


class HttpOK(MockResponse):
    """
    A response that returns status code 200
    """
    def __init__(self, content=None):
        super(HttpOK, self).__init__(status_code=200, content=content)
