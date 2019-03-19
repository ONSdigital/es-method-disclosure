from . import Disclosure

def test_Disclosure():
    assert Disclosure.apply("Jane") == "hello Jane"
