from typing import Iterable, Optional, Union

from tests.test_handlers import TestCLPBase
import unittest


def load_tests(
    loader: unittest.TestLoader,
    tests: Iterable[Union[unittest.TestCase, unittest.TestSuite]],
    pattern: Optional[str],
) -> unittest.TestSuite:
    suite: unittest.TestSuite = unittest.TestSuite()
    for test_class in TestCLPBase.__subclasses__():
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
