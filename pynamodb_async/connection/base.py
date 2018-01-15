import asyncio
import logging
import random
import uuid

import time
from aiobotocore import get_session
from botocore.exceptions import BotoCoreError, ClientError
from botocore.vendored import requests
from botocore.vendored.requests import Request
from pynamodb.compat import NullHandler

from pynamodb.connection import base
from pynamodb.connection.base import BOTOCORE_EXCEPTIONS, MetaTable
from pynamodb.connection.util import pythonic
from pynamodb.constants import SERVICE_NAME, TABLE_NAME, ITEM, CONDITION_EXPRESSION, EXPRESSION_ATTRIBUTE_NAMES, \
    EXPRESSION_ATTRIBUTE_VALUES, PUT_ITEM, AND, DESCRIBE_TABLE, LIST_TABLES, UPDATE_TABLE, DELETE_TABLE, CREATE_TABLE, \
    RETURN_CONSUMED_CAPACITY, TOTAL, CONSUMED_CAPACITY, CAPACITY_UNITS, PROVISIONED_THROUGHPUT, READ_CAPACITY_UNITS, \
    WRITE_CAPACITY_UNITS, ATTR_NAME, ATTR_TYPE, ATTR_DEFINITIONS, INDEX_NAME, KEY_SCHEMA, PROJECTION, KEY_TYPE, \
    GLOBAL_SECONDARY_INDEXES, LOCAL_SECONDARY_INDEXES, STREAM_SPECIFICATION, STREAM_ENABLED, STREAM_VIEW_TYPE, TABLE_KEY
from pynamodb.exceptions import PutError, TableError, VerboseClientError, TableDoesNotExist

from pynamodb_async.settings import get_settings_value

log = logging.getLogger(__name__)
log.addHandler(NullHandler())


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

    async def close_session(self):
        self.client.close()

    async def _make_api_call(self, operation_name, operation_kwargs):
        """
        This private method is here for two reasons:
        1. It's faster to avoid using botocore's response parsing
        2. It provides a place to monkey patch requests for unit testing
        """
        operation_model = self.client._service_model.operation_model(operation_name)
        request_dict = self.client._convert_to_request_dict(
            operation_kwargs,
            operation_model
        )
        prepared_request = self._create_prepared_request(request_dict, operation_model)

        for i in range(0, self._max_retry_attempts_exception + 1):
            attempt_number = i + 1
            is_last_attempt_for_exceptions = i == self._max_retry_attempts_exception

            try:
                response = self.requests_session.send(
                    prepared_request,
                    timeout=self._request_timeout_seconds,
                    proxies=self.client._endpoint.proxies,
                )
                data = response.json()
            except (requests.RequestException, ValueError) as e:
                if is_last_attempt_for_exceptions:
                    log.debug('Reached the maximum number of retry attempts: %s', attempt_number)
                    raise
                else:
                    # No backoff for fast-fail exceptions that likely failed at the frontend
                    log.debug(
                        'Retry needed for (%s) after attempt %s, retryable %s caught: %s',
                        operation_name,
                        attempt_number,
                        e.__class__.__name__,
                        e
                    )
                    continue

            if response.status_code >= 300:
                # Extract error code from __type
                code = data.get('__type', '')
                if '#' in code:
                    code = code.rsplit('#', 1)[1]
                botocore_expected_format = {'Error': {'Message': data.get('message', ''), 'Code': code}}
                verbose_properties = {
                    'request_id': response.headers.get('x-amzn-RequestId')
                }

                if 'RequestItems' in operation_kwargs:
                    # Batch operations can hit multiple tables, report them comma separated
                    verbose_properties['table_name'] = ','.join(operation_kwargs['RequestItems'])
                else:
                    verbose_properties['table_name'] = operation_kwargs.get('TableName')

                try:
                    raise VerboseClientError(botocore_expected_format, operation_name, verbose_properties)
                except VerboseClientError as e:
                    if is_last_attempt_for_exceptions:
                        log.debug('Reached the maximum number of retry attempts: %s', attempt_number)
                        raise
                    elif response.status_code < 500 and code != 'ProvisionedThroughputExceededException':
                        # We don't retry on a ConditionalCheckFailedException or other 4xx (except for
                        # throughput related errors) because we assume they will fail in perpetuity.
                        # Retrying when there is already contention could cause other problems
                        # in part due to unnecessary consumption of throughput.
                        raise
                    else:
                        # We use fully-jittered exponentially-backed-off retries:
                        #  https://www.awsarchitectureblog.com/2015/03/backoff.html
                        sleep_time_ms = random.randint(0, self._base_backoff_ms * (2 ** i))
                        log.debug(
                            'Retry with backoff needed for (%s) after attempt %s,'
                            'sleeping for %s milliseconds, retryable %s caught: %s',
                            operation_name,
                            attempt_number,
                            sleep_time_ms,
                            e.__class__.__name__,
                            e
                        )
                        time.sleep(sleep_time_ms / 1000.0)
                        continue

            return self._handle_binary_attributes(data)

    async def dispatch(self, operation_name, operation_kwargs):
        """
        Dispatches `operation_name` with arguments `operation_kwargs`

        Raises TableDoesNotExist if the specified table does not exist
        """
        if operation_name not in [DESCRIBE_TABLE, LIST_TABLES, UPDATE_TABLE, DELETE_TABLE, CREATE_TABLE]:
            if RETURN_CONSUMED_CAPACITY not in operation_kwargs:
                operation_kwargs.update(self.get_consumed_capacity_map(TOTAL))
        self._log_debug(operation_name, operation_kwargs)

        # TODO signals will be implemented.
        # self.send_pre_boto_callback(operation_name, req_uuid, table_name)
        data = await self._make_api_call(operation_name, operation_kwargs)
        # self.send_post_boto_callback(operation_name, req_uuid, table_name)

        if data and CONSUMED_CAPACITY in data:
            capacity = data.get(CONSUMED_CAPACITY)
            if isinstance(capacity, dict) and CAPACITY_UNITS in capacity:
                capacity = capacity.get(CAPACITY_UNITS)
            log.debug("%s %s consumed %s units", data.get(TABLE_NAME, ''), operation_name, capacity)
        return data

    async def describe_table(self, table_name):
        """
        Performs the DescribeTable operation
        """
        try:
            tbl = await self.get_meta_table(table_name, refresh=True)
            if tbl:
                return tbl.data
        except ValueError:
            pass
        raise TableDoesNotExist(table_name)

    async def get_meta_table(self, table_name, refresh=False):
        """
        Returns a MetaTable
        """
        if table_name not in self._tables or refresh:
            operation_kwargs = {
                TABLE_NAME: table_name
            }
            try:
                data = await self.dispatch(DESCRIBE_TABLE, operation_kwargs)
                self._tables[table_name] = MetaTable(data.get(TABLE_KEY))
            except BotoCoreError as e:
                raise TableError("Unable to describe table: {0}".format(e), e)
            except ClientError as e:
                if 'ResourceNotFound' in e.response['Error']['Code']:
                    raise TableDoesNotExist(e.response['Error']['Message'])
                else:
                    raise
        return self._tables[table_name]

    async def create_table(self,
                           table_name,
                           attribute_definitions=None,
                           key_schema=None,
                           read_capacity_units=None,
                           write_capacity_units=None,
                           global_secondary_indexes=None,
                           local_secondary_indexes=None,
                           stream_specification=None):
        """
        Performs the CreateTable operation
        """
        operation_kwargs = {
            TABLE_NAME: table_name,
            PROVISIONED_THROUGHPUT: {
                READ_CAPACITY_UNITS: read_capacity_units,
                WRITE_CAPACITY_UNITS: write_capacity_units
            }
        }
        attrs_list = []
        if attribute_definitions is None:
            raise ValueError("attribute_definitions argument is required")
        for attr in attribute_definitions:
            attrs_list.append({
                ATTR_NAME: attr.get(pythonic(ATTR_NAME)),
                ATTR_TYPE: attr.get(pythonic(ATTR_TYPE))
            })
        operation_kwargs[ATTR_DEFINITIONS] = attrs_list

        if global_secondary_indexes:
            global_secondary_indexes_list = []
            for index in global_secondary_indexes:
                global_secondary_indexes_list.append({
                    INDEX_NAME: index.get(pythonic(INDEX_NAME)),
                    KEY_SCHEMA: sorted(index.get(pythonic(KEY_SCHEMA)), key=lambda x: x.get(KEY_TYPE)),
                    PROJECTION: index.get(pythonic(PROJECTION)),
                    PROVISIONED_THROUGHPUT: index.get(pythonic(PROVISIONED_THROUGHPUT))
                })
            operation_kwargs[GLOBAL_SECONDARY_INDEXES] = global_secondary_indexes_list

        if key_schema is None:
            raise ValueError("key_schema is required")
        key_schema_list = []
        for item in key_schema:
            key_schema_list.append({
                ATTR_NAME: item.get(pythonic(ATTR_NAME)),
                KEY_TYPE: str(item.get(pythonic(KEY_TYPE))).upper()
            })
        operation_kwargs[KEY_SCHEMA] = sorted(key_schema_list, key=lambda x: x.get(KEY_TYPE))

        local_secondary_indexes_list = []
        if local_secondary_indexes:
            for index in local_secondary_indexes:
                local_secondary_indexes_list.append({
                    INDEX_NAME: index.get(pythonic(INDEX_NAME)),
                    KEY_SCHEMA: sorted(index.get(pythonic(KEY_SCHEMA)), key=lambda x: x.get(KEY_TYPE)),
                    PROJECTION: index.get(pythonic(PROJECTION)),
                })
            operation_kwargs[LOCAL_SECONDARY_INDEXES] = local_secondary_indexes_list

        if stream_specification:
            operation_kwargs[STREAM_SPECIFICATION] = {
                STREAM_ENABLED: stream_specification[pythonic(STREAM_ENABLED)],
                STREAM_VIEW_TYPE: stream_specification[pythonic(STREAM_VIEW_TYPE)]
            }

        try:
            data = await self.dispatch(CREATE_TABLE, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise TableError("Failed to create table: {0}".format(e), e)
        return data

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
