import asyncio

import uuid

from pynamodb.attributes import NumberAttribute, UnicodeAttribute

from pynamodb_async.models import Model
import tests.test_settings as settings


def get_test_table_model(unique_id):
    class TableTestModel(Model):
        class Meta:
            table_name = 'test_table_{0}'.format(unique_id)
            host = settings.DYNAMODB_HOST

        temp_id = NumberAttribute(hash_key=True)
        name = UnicodeAttribute()
        description = UnicodeAttribute()

    return TableTestModel


def main():
    loop = asyncio.get_event_loop()
    tasks = list()
    try:
        for i in range(20):
            test_model = get_test_table_model(uuid.uuid4())
            tasks.append(test_model.create_table(read_capacity_units=3, write_capacity_units=3))
    except KeyboardInterrupt:
        pass

    task_group = asyncio.gather(*tasks)
    results = loop.run_until_complete(task_group)


if __name__ == "__main__":
    main()
