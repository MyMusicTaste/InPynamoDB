import asyncio

from aiobotocore import get_session
from botocore.vendored.requests import Request

from pynamodb.connection import base
from pynamodb.constants import SERVICE_NAME
from pynamodb_async.settings import get_settings_value


class AsyncConnection(base.Connection):
    # TODO need to override my methods
    def __init__(self, region=None, host=None, session_cls=None, loop=None, request_timeout_seconds=None,
                 max_retry_attempts=None, base_backoff_ms=None):
        super().__init__(region, host, session_cls, request_timeout_seconds, max_retry_attempts, base_backoff_ms)

        self.loop = loop

        if loop is None:
            self.loop = asyncio.get_event_loop()

    @property
    def session(self):
        """
        Returns a valid aiobotocore session
        """
        if self._session is None:
            self._session = get_session()
        return self._session

    @property
    def requests_session(self):
        """
        Return a requests session to execute prepared requests using the same pool
        """
        if self._requests_session is None:
            self._requests_session = self.session_cls()
        return self._requests_session

    @property
    def client(self):
        """
        Returns a aiobotocore dynamodb client
        """
        # botocore has a known issue where it will cache empty credentials
        # https://github.com/boto/botocore/blob/4d55c9b4142/botocore/credentials.py#L1016-L1021
        # if the client does not have credentials, we create a new client
        # otherwise the client is permanently poisoned in the case of metadata service flakiness when using IAM roles
        if not self._client or (self._client._request_signer and not self._client._request_signer._credentials):
            self._client = self.session.create_client(SERVICE_NAME, self.region, endpoint_url=self.host)
        return self._client

    def _create_prepared_request(self, request_dict, operation_model):
        boto_prepared_request = self.client._endpoint.create_request(request_dict, operation_model)

        # The call requests_session.send(final_prepared_request) ignores the headers which are
        # part of the request session. In order to include the requests session headers inside
        # the request, we create a new request object, and call prepare_request with the newly
        # created request object
        raw_request_with_params = Request(
            boto_prepared_request.method,
            boto_prepared_request.url,
            data=boto_prepared_request.body,
            headers=boto_prepared_request.headers
        )

        return self.requests_session.prepare_request(raw_request_with_params)
