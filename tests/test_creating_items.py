import asyncio
import random

import time
from pynamodb.attributes import NumberAttribute, UnicodeAttribute

from tests import test_settings as settings
from pynamodb_async.models import Model
from pynamodb.models import Model as PynamoDBModel


def get_test_table_model(temp_id, name, description):
    class TableTestModel(Model):
        class Meta:
            table_name = 'test_table'
            host = settings.DYNAMODB_HOST

        temp_id = NumberAttribute(hash_key=True)
        name = UnicodeAttribute()
        description = UnicodeAttribute()

        async def save(self, condition=None, conditional_operator=None, **expected_values):
            asyncio.sleep(0.5)  # possible IO blocking since latency
            print("STARTED: Model id " + str(self.temp_id))
            await super(TableTestModel, self).save(condition, conditional_operator, **expected_values)
            print("ENDED: Model id " + str(self.temp_id))

    return TableTestModel(temp_id=temp_id, name=name, description=description)


def sync_main():
    tasks = list()

    class TestSyncTable(PynamoDBModel):
        class Meta:
            table_name = 'test_sync_table'
            host = settings.DYNAMODB_HOST

        temp_id = NumberAttribute(hash_key=True)
        name = UnicodeAttribute()
        description = UnicodeAttribute()

        def save(self, condition=None, conditional_operator=None, **expected_values):
            time.sleep(0.5)  # possible IO blocking since latency
            print("STARTED: Model id " + str(self.temp_id))
            super(TestSyncTable, self).save(condition, conditional_operator, **expected_values)
            print("ENDED: Model id " + str(self.temp_id))

    if not TestSyncTable.exists():
        TestSyncTable.create_table(read_capacity_units=3, write_capacity_units=3)

    for i in range(100):
        time.sleep(0.5)
        temp_id = i
        name = ''.join(random.choice('0123456789ABCDEF') for i in range(16))

        test_model = TestSyncTable(temp_id=temp_id, name=name, description=(name + " - " + str(temp_id)))

        test_model.save()


def async_main():
    loop = asyncio.get_event_loop()
    tasks = list()
    try:
        for i in range(100):
            temp_id = i
            name = ''.join(random.choice('0123456789ABCDEF') for i in range(16))

            test_model = get_test_table_model(temp_id=temp_id, name=name, description=(name + " - " + str(temp_id)))

            tasks.append(test_model.save())
    except KeyboardInterrupt:
        pass

    task_group = asyncio.gather(*tasks)
    _start_time = time.time()
    print("ASYNC_MAIN: Starts at " + str(_start_time))
    results = loop.run_until_complete(task_group)
    _end_time = time.time()
    print("ASYNC_MAIN: Ends at " + str(_end_time))
    print("ASYNC_ELAPSED_TIME: " + str(_end_time - _start_time))


if __name__ == "__main__":
    async_main()
    print('\n\n')
    start_time = time.time()
    print("SYNC_MAIN: Starts at " + str(start_time))
    sync_main()
    end_time = time.time()
    print("SYNC_MAIN: Ends at " + str(end_time))
    print("SYNC_ELAPSED_TIME: " + str(end_time - start_time))
