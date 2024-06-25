import unittest
from code import Calculations

# For an unittest to identify the tests, the method's name should start with word test

class TestCalculations(unittest.TestCase):

    # def setUp(self):
    #     self.calculation = Calculations(10, 20)

    @classmethod
    def setUpClass(self):
        self.calculation = Calculations(10, 20)

    def test_sum(self) -> None:
        self.assertEqual(self.calculation.get_sum(), 30, "The sum is wrong")

    def test_difference(self) ->None:
        self.assertEqual(self.calculation.get_difference(), -10, "The difference is wrong")

    def test_product(self) -> None:
        self.assertEqual(self.calculation.get_product(), 200, "The product is wrong")

    def not_test_quotient(self) -> None:
        self.assertEqual(self.calculation.get_quotient(), 0.5, "The quotient is wrong")


class TestAbsFunctions(unittest.TestCase):
    def test_positive_number(self) -> None:
        self.assertEqual(abs(10), 10)

    def test_negative_number(self) -> None:
        self.assertEqual(abs(-10), 10)

    def test_zero(self) -> None:
        self.assertEqual(abs(0), 0)


if __name__ == '__main__':
    unittest.main()

