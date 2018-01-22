from tests.test_mockup_model import TestComplexModelAsync


async def async_main():
    async_model = TestComplexModelAsync.create()
    async_model.name.set("hello world!")