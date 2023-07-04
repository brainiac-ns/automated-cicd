import unittest

from scripts.script_2 import run


class TestScript2(unittest.TestCase):
    def test_input_number(self):
        self.assertEqual(2, run(1))

    def test_input_string(self):
        self.assertEqual("11", run("1"))
