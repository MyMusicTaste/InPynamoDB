import asyncio
import random

import time

from tests.test_mockup_model import TestModelSync, TestModelAsync


def sync_main():
    for i in range(100):
        temp_id = i
        name = ''.join(random.choice('0123456789ABCDEF') for i in range(16))

        test_model = TestModelSync(temp_id=temp_id, name=name, description=(name + " - " + str(temp_id)))

        test_model.save()


async def async_main():
    tasks = list()

    for i in range(100):
        temp_id = i
        name = ''.join(random.choice('0123456789ABCDEF') for i in range(16))

        test_model = await TestModelAsync.create(temp_id=temp_id, name=name, description=(name + " - " + str(temp_id)))

        tasks.append(test_model.save())

    task_group = asyncio.gather(*tasks)
    _start_time = time.time()
    print("ASYNC_MAIN: Starts at " + str(_start_time))
    await task_group
    _end_time = time.time()
    print("ASYNC_MAIN: Ends at " + str(_end_time))
    print("ASYNC_ELAPSED_TIME: " + str(_end_time - _start_time))


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_main())
    print('\n\n')
    start_time = time.time()
    print("SYNC_MAIN: Starts at " + str(start_time))
    sync_main()
    end_time = time.time()
    print("SYNC_MAIN: Ends at " + str(end_time))
    print("SYNC_ELAPSED_TIME: " + str(end_time - start_time))

# TEST: 1. Test with existent table
# TEST: 2. Test with non-existent table - should be error but we can check how it works asynchronously
