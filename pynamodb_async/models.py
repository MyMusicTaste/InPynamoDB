import asyncio
import time
from pynamodb.connection.util import pythonic
from pynamodb.exceptions import TableDoesNotExist
from pynamodb.models import Model as PynamoDBModel

from pynamodb_async.connection.table import TableConnection
from pynamodb_async.constants import PUT_FILTER_OPERATOR_MAP, READ_CAPACITY_UNITS, WRITE_CAPACITY_UNITS, \
    STREAM_VIEW_TYPE, STREAM_SPECIFICATION, STREAM_ENABLED, GLOBAL_SECONDARY_INDEXES, LOCAL_SECONDARY_INDEXES, \
    ATTR_DEFINITIONS, ATTR_NAME


class Model(PynamoDBModel):
    # TODO Check CRUD first
    @classmethod
    async def create_table(cls, wait=False, read_capacity_units=None, write_capacity_units=None):
        """
        :param wait: argument for making this method identical to PynamoDB, but not-used variable
        :param read_capacity_units: Sets the read capacity units for this table
        :param write_capacity_units: Sets the write capacity units for this table
        """
        if not await cls.exists():
            schema = cls._get_schema()
            if hasattr(cls.Meta, pythonic(READ_CAPACITY_UNITS)):
                schema[pythonic(READ_CAPACITY_UNITS)] = cls.Meta.read_capacity_units
            if hasattr(cls.Meta, pythonic(WRITE_CAPACITY_UNITS)):
                schema[pythonic(WRITE_CAPACITY_UNITS)] = cls.Meta.write_capacity_units
            if hasattr(cls.Meta, pythonic(STREAM_VIEW_TYPE)):
                schema[pythonic(STREAM_SPECIFICATION)] = {
                    pythonic(STREAM_ENABLED): True,
                    pythonic(STREAM_VIEW_TYPE): cls.Meta.stream_view_type
                }
            if read_capacity_units is not None:
                schema[pythonic(READ_CAPACITY_UNITS)] = read_capacity_units
            if write_capacity_units is not None:
                schema[pythonic(WRITE_CAPACITY_UNITS)] = write_capacity_units
            index_data = cls._get_indexes()
            schema[pythonic(GLOBAL_SECONDARY_INDEXES)] = index_data.get(pythonic(GLOBAL_SECONDARY_INDEXES))
            schema[pythonic(LOCAL_SECONDARY_INDEXES)] = index_data.get(pythonic(LOCAL_SECONDARY_INDEXES))
            index_attrs = index_data.get(pythonic(ATTR_DEFINITIONS))
            attr_keys = [attr.get(pythonic(ATTR_NAME)) for attr in schema.get(pythonic(ATTR_DEFINITIONS))]
            for attr in index_attrs:
                attr_name = attr.get(pythonic(ATTR_NAME))
                if attr_name not in attr_keys:
                    schema[pythonic(ATTR_DEFINITIONS)].append(attr)
                    attr_keys.append(attr_name)
            result = await cls._get_connection().create_table(
                **schema
            )

            await cls._get_connection().connection.close_session()

            return result

    @classmethod
    async def exists(cls):
        """
        Returns True if this table exists, False otherwise
        """
        try:
            await cls._get_connection().describe_table()
            return True
        except TableDoesNotExist:
            return False

    async def save(self, condition=None, conditional_operator=None, **expected_values):
        """
        Save this object to dynamodb
        """
        self._conditional_operator_check(conditional_operator)
        args, kwargs = self._get_save_args()
        if len(expected_values):
            kwargs.update(expected=self._build_expected_values(expected_values, PUT_FILTER_OPERATOR_MAP))
        kwargs.update(conditional_operator=conditional_operator)
        kwargs.update(condition=condition)
        return await self._get_connection().put_item(*args, **kwargs)

    @classmethod
    def _get_connection(cls):
        """
        Returns a (cached) connection
        """
        if not hasattr(cls, "Meta") or cls.Meta.table_name is None:
            raise AttributeError(
                """As of v1.0 PynamoDB Models require a `Meta` class.
                See https://pynamodb.readthedocs.io/en/latest/release_notes.html"""
            )
        if cls._connection is None:
            cls._connection = TableConnection(cls.Meta.table_name,
                                              region=cls.Meta.region,
                                              host=cls.Meta.host,
                                              session_cls=cls.Meta.session_cls,
                                              request_timeout_seconds=cls.Meta.request_timeout_seconds,
                                              max_retry_attempts=cls.Meta.max_retry_attempts,
                                              base_backoff_ms=cls.Meta.base_backoff_ms)
        return cls._connection
