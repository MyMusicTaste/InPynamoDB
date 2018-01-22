import asyncio

import time

from tests.test_mockup_model import TestModelAsync, TestModelSync


def sync_main():
    for i in range(10000):
        TestModelSync.query(hash_key=i)


async def async_main():
    _start_time = time.time()
    print("ASYNC_MAIN: Starts at " + str(_start_time))
    for i in range(10000):
        await TestModelAsync.query(hash_key=i)

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
