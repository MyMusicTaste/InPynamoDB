import asyncio

import time

import aiohttp
from pynamodb.attributes import NumberAttribute, UnicodeAttribute, ListAttribute

from tests import test_settings as settings

from pynamodb_async.models import Model
from pynamodb.models import Model as PynamoDBModel


class TestModelAsync(Model):
    class Meta:
        table_name = 'test_model_async'
        host = settings.DYNAMODB_HOST

    temp_id = NumberAttribute(hash_key=True)
    name = UnicodeAttribute()
    description = UnicodeAttribute()


class TestModelSync(PynamoDBModel):
        class Meta:
            table_name = 'test_model_sync'
            host = settings.DYNAMODB_HOST

        temp_id = NumberAttribute(hash_key=True)
        name = UnicodeAttribute()
        description = UnicodeAttribute()

        def __init__(self, hash_key=None, range_key=None, **attributes):
            if not self.exists():
                self.create_table(read_capacity_units=3, write_capacity_units=3)

            super(TestModelSync, self).__init__(hash_key, range_key, **attributes)


class TestComplexModelAsync(Model):
    class Meta:
        table_name = 'test_complex_model_async'
        host = settings.DYNAMODB_HOST

    id = NumberAttribute(hash_key=True)
    list_attr = ListAttribute(default=[])
    name = UnicodeAttribute()
    description = UnicodeAttribute()


class TestComplexModelSync(PynamoDBModel):
    class Meta:
        table_name = 'test_complex_model_sync'
        host = settings.DYNAMODB_HOST

    id = NumberAttribute(hash_key=True)
    list_attr = ListAttribute(default=[])
    name = UnicodeAttribute()
    description = UnicodeAttribute()
