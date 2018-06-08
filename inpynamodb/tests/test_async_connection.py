import pytest
from asynctest import patch, mock
from botocore.exceptions import BotoCoreError, ClientError
from pynamodb.constants import DEFAULT_REGION
from pynamodb.exceptions import TableError, TableDoesNotExist
from pynamodb.tests.data import DESCRIBE_TABLE_DATA, LIST_TABLE_DATA

from inpynamodb.connection.base import AsyncConnection


PATCH_METHOD = 'aiobotocore.client.AioBaseClient._make_api_call'


class TestAsyncConnection:
    test_table_name = 'ci-table'
    region = DEFAULT_REGION

    def test_get_client(self):
        conn = AsyncConnection()
        assert conn is not None
        conn = AsyncConnection(host='http://foohost')
        assert conn.client is not None
        assert conn is not None
        assert f"AsyncConnection<{conn.host}>" == repr(conn)

    def test_subsequent_client_is_not_cached_when_credentials_none(self):
        with patch('inpynamodb.connection.AsyncConnection.session') as session_mock:
            session_mock.create_client.return_value._request_signer._credentials = None
            conn = AsyncConnection()

            # make two calls to .client property, expect two calls to create client
            assert conn.client is not None
            assert conn.client is not None

            session_mock.create_client.assert_has_calls(
                [
                    mock.call('dynamodb', 'us-east-1', endpoint_url=None),
                    mock.call('dynamodb', 'us-east-1', endpoint_url=None),
                ],
                any_order=True
            )

    def test_subsequent_client_is_cached_when_credentials_truthy(self):
        with patch('inpynamodb.connection.AsyncConnection.session') as session_mock:
            session_mock.create_client.return_value._request_signer._credentials = True
            conn = AsyncConnection()

            # make two calls to .client property, expect one call to create client
            assert conn.client is not None
            assert conn.client is not None

            assert session_mock.create_client.mock_calls.count(
                mock.call('dynamodb', 'us-east-1', endpoint_url=None)
            ) == 1

    @pytest.mark.asyncio
    async def test_create_table(self):
        """
        AsyncConnection.create_table
        """
        conn = AsyncConnection(self.region)
        kwargs = {
            'read_capacity_units': 1,
            'write_capacity_units': 1,
        }

        with pytest.raises(ValueError):
            await conn.create_table(self.test_table_name, **kwargs)

        with pytest.raises(ValueError):
            await conn.create_table(self.test_table_name, **kwargs)

        kwargs['attribute_definitions'] = [
            {
                'attribute_name': 'key1',
                'attribute_type': 'S'
            },
            {
                'attribute_name': 'key2',
                'attribute_type': 'S'
            }
        ]
        with pytest.raises(ValueError):
            await conn.create_table(self.test_table_name, **kwargs)

        kwargs['key_schema'] = [
            {
                'attribute_name': 'key1',
                'key_type': 'hash'
            },
            {
                'attribute_name': 'key2',
                'key_type': 'range'
            }
        ]
        params = {
            'TableName': 'ci-table',
            'ProvisionedThroughput': {
                'WriteCapacityUnits': 1,
                'ReadCapacityUnits': 1
            },
            'AttributeDefinitions': [
                {
                    'AttributeType': 'S',
                    'AttributeName': 'key1'
                },
                {
                    'AttributeType': 'S',
                    'AttributeName': 'key2'
                }
            ],
            'KeySchema': [
                {
                    'KeyType': 'HASH',
                    'AttributeName': 'key1'
                },
                {
                    'KeyType': 'RANGE',
                    'AttributeName': 'key2'
                }
            ]
        }
        with patch(PATCH_METHOD) as req:
            req.side_effect = BotoCoreError
            with pytest.raises(TableError):
                await conn.create_table(self.test_table_name, **kwargs)

        with patch(PATCH_METHOD) as req:
            req.return_value = None
            await conn.create_table(self.test_table_name, **kwargs)
            assert req.call_args[0][1] == params

        kwargs['global_secondary_indexes'] = [
            {
                'index_name': 'alt-index',
                'key_schema': [
                    {
                        'KeyType': 'HASH',
                        'AttributeName': 'AltKey'
                    }
                ],
                'projection': {
                    'ProjectionType': 'KEYS_ONLY'
                },
                'provisioned_throughput': {
                    'ReadCapacityUnits': 1,
                    'WriteCapacityUnits': 1,
                },
            }
        ]
        params['GlobalSecondaryIndexes'] = [{'IndexName': 'alt-index', 'Projection': {'ProjectionType': 'KEYS_ONLY'},
                                               'KeySchema': [{'AttributeName': 'AltKey', 'KeyType': 'HASH'}],
                                               'ProvisionedThroughput': {'ReadCapacityUnits': 1,
                                                                         'WriteCapacityUnits': 1}}]
        with patch(PATCH_METHOD) as req:
            req.return_value = None
            await conn.create_table(self.test_table_name, **kwargs)

            # Ensure that the hash key is first when creating indexes
            assert req.call_args[0][1]['GlobalSecondaryIndexes'][0]['KeySchema'][0]['KeyType'] == 'HASH'
            assert req.call_args[0][1] == params

        del(kwargs['global_secondary_indexes'])
        del(params['GlobalSecondaryIndexes'])

        kwargs['local_secondary_indexes'] = [
            {
                'index_name': 'alt-index',
                'projection': {
                    'ProjectionType': 'KEYS_ONLY'
                },
                'key_schema': [
                    {
                        'AttributeName': 'AltKey', 'KeyType': 'HASH'
                    }
                ],
                'provisioned_throughput': {
                    'ReadCapacityUnits': 1,
                    'WriteCapacityUnits': 1
                }
            }
        ]
        params['LocalSecondaryIndexes'] = [
            {
                'Projection': {
                    'ProjectionType': 'KEYS_ONLY'
                },
                'KeySchema': [
                    {
                        'KeyType': 'HASH',
                        'AttributeName': 'AltKey'
                    }
                ],
                'IndexName': 'alt-index'
            }
        ]
        with patch(PATCH_METHOD) as req:
            req.return_value = None
            await conn.create_table(self.test_table_name, **kwargs)
            assert req.call_args[0][1] == params

        kwargs['stream_specification'] = {
                'stream_enabled': True,
                'stream_view_type': 'NEW_IMAGE'
        }
        params['StreamSpecification'] = {
                'StreamEnabled': True,
                'StreamViewType': 'NEW_IMAGE'
        }
        with patch(PATCH_METHOD) as req:
            req.return_value = None
            await conn.create_table(self.test_table_name, **kwargs)
            assert req.call_args[0][1] == params

    @pytest.mark.asyncio
    async def test_delete_table(self):
        """
        AsyncConnection.delete_table
        """
        params = {'TableName': 'ci-table'}
        with patch(PATCH_METHOD) as req:
            req.return_value = None
            conn = AsyncConnection(self.region)
            await conn.delete_table(self.test_table_name)
            kwargs = req.call_args[0][1]
            assert kwargs == params

        with patch(PATCH_METHOD) as req:
            req.side_effect = BotoCoreError
            conn = AsyncConnection(self.region)
            with pytest.raises(TableError):
                await conn.delete_table(self.test_table_name)

    @pytest.mark.asyncio
    async def test_update_table(self):
        """
        AsyncConnection.update_table
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = None
            conn = AsyncConnection(self.region)
            params = {
                'ProvisionedThroughput': {
                    'WriteCapacityUnits': 2,
                    'ReadCapacityUnits': 2
                },
                'TableName': 'ci-table'
            }
            await conn.update_table(
                self.test_table_name,
                read_capacity_units=2,
                write_capacity_units=2
            )
            assert req.call_args[0][1] == params

        with pytest.raises(ValueError):
            await conn.update_table(self.test_table_name, read_capacity_units=2)

        with patch(PATCH_METHOD) as req:
            req.side_effect = BotoCoreError
            conn = AsyncConnection(self.region)
            with pytest.raises(TableError):
                await conn.update_table(self.test_table_name, read_capacity_units=2, write_capacity_units=2)

        with patch(PATCH_METHOD) as req:
            req.return_value = None
            conn = AsyncConnection(self.region)

            global_secondary_index_updates = [
                {
                    "index_name": "foo-index",
                    "read_capacity_units": 2,
                    "write_capacity_units": 2
                }
            ]
            params = {
                'TableName': 'ci-table',
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 2,
                    'WriteCapacityUnits': 2,
                },
                'GlobalSecondaryIndexUpdates': [
                    {
                        'Update': {
                            'IndexName': 'foo-index',
                            'ProvisionedThroughput': {
                                'ReadCapacityUnits': 2,
                                'WriteCapacityUnits': 2,
                            }
                        }
                    }

                ]
            }

            await conn.update_table(
                self.test_table_name,
                read_capacity_units=2,
                write_capacity_units=2,
                global_secondary_index_updates=global_secondary_index_updates
            )
            assert req.call_args[0][1] == params

    @pytest.mark.asyncio
    async def test_describe_table(self):
        """
        Connection.describe_table
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = DESCRIBE_TABLE_DATA
            conn = AsyncConnection(self.region)
            await conn.describe_table(self.test_table_name)
            assert req.call_args[0][1] == {'TableName': 'ci-table'}

        with pytest.raises(TableDoesNotExist):
            with patch(PATCH_METHOD) as req:
                req.side_effect = ClientError(
                    {'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Not Found'}}, "DescribeTable"
                )
                conn = AsyncConnection(self.region)
                await conn.describe_table(self.test_table_name)

        with pytest.raises(TableDoesNotExist):
            with patch(PATCH_METHOD) as req:
                req.side_effect = ValueError()
                conn = AsyncConnection(self.region)
                await conn.describe_table(self.test_table_name)

    @pytest.mark.asyncio
    async def test_list_tables(self):
        """
        AsyncConnection.list_tables
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = LIST_TABLE_DATA
            conn = AsyncConnection(self.region)
            await conn.list_tables(exclusive_start_table_name='Thread')
            assert req.call_args[0][1] == {'ExclusiveStartTableName': 'Thread'}

        with patch(PATCH_METHOD) as req:
            req.return_value = LIST_TABLE_DATA
            conn = AsyncConnection(self.region)
            await conn.list_tables(limit=3)
            assert req.call_args[0][1] == {'Limit': 3}

        with patch(PATCH_METHOD) as req:
            req.return_value = LIST_TABLE_DATA
            conn = AsyncConnection(self.region)
            await conn.list_tables()
            assert req.call_args[0][1] == {}

        with patch(PATCH_METHOD) as req:
            req.side_effect = BotoCoreError
            conn = AsyncConnection(self.region)
            with pytest.raises(TableError):
                await conn.list_tables()
