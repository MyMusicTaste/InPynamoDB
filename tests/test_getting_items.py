import asyncio

from tests.test_mockup_model import TestModelAsync, TestModelSync


def sync_main():
    pass


async def async_main():
    pass


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_main())

    # for item in loop.run_until_complete(TestModelAsync.query()):
    #     print(item)
