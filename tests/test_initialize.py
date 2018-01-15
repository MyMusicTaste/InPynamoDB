from tests.test_model import TestModel

if __name__ == "__main__":
    if not TestModel.exists():
        print("TESTING Table does not exists... Creating...")
        TestModel.create_table(read_capacity_units=3, write_capacity_units=3, wait=False)
