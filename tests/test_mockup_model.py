import asyncio

import time

import aiohttp
from pynamodb.attributes import NumberAttribute, UnicodeAttribute

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

    async def save(self, condition=None, conditional_operator=None, **expected_values):
        # await asyncio.sleep(0.05)  # possible IO blocking - 50ms
        # print("STARTED: Model id " + str(self.temp_id))
        await super(TestModelAsync, self).save(condition, conditional_operator, **expected_values)
        # print("ENDED: Model id " + str(self.temp_id))


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

        def save(self, condition=None, conditional_operator=None, **expected_values):
            # time.sleep(0.05)  # possible IO blocking - 50ms
            # print("STARTED: Model id " + str(self.temp_id))
            super(TestModelSync, self).save(condition, conditional_operator, **expected_values)
            # print("ENDED: Model id " + str(self.temp_id))