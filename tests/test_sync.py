import time

from tests.test_model import TestModel


if __name__ == "__main__":
    test_models = list()
    for i in range(100):
        test_models.append(TestModel(temp_id=i, name="Tester - " + str(i), description="{0} Tester".format(str(i))))

    print("Starting to create testers...")
    start_time = time.time()
    for test_model in test_models:
        test_model.save()
        print(str(test_model.temp_id) + ' created.')
    end_time = time.time()
    print("Creating Testers ended.")
    print("=======================")
    print("start_time = " + str(start_time))
    print("end_time = " + str(end_time))
    print("Elapsed time: " + str(end_time - start_time) + ' sec.')
