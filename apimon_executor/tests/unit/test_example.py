import unittest


class BaseClientTestCase(unittest.TestCase):

    def setUp(self):
        super(BaseClientTestCase, self).setUp()

    def test_example(self):
        self.assertTrue(True)

