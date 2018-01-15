import asyncio
import time

from tests.test_model import TestModel


async def main(_test_models):
    loop = asyncio.get_event_loop()
    futures = [loop.run_in_executor(None, test_model.save()) for test_model in test_models]

    for result in await asyncio.gather(futures):
        print(result)


if __name__ == "__main__":
    test_models = list()
    for i in range(100):
        test_models.append(TestModel(temp_id=i, name="Tester - " + str(i), description="{0} Tester".format(str(i))))

    print("Starting to create testers...")
    start_time = time.time()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(test_models))
    end_time = time.time()
    print("Creating Testers ended.")
    print("=======================")
    print("start_time = " + str(start_time))
    print("end_time = " + str(end_time))
    print("Elapsed time: " + str(end_time - start_time) + ' sec.')
