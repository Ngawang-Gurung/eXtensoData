import unittest
from calc import Calculations

# For an unittest to identify the tests, the method's name should start with word test

class TestCalculations(unittest.TestCase):

    # def setUp(self):
    #     self.calculation = calc.Calculations(10, 20)

    @classmethod
    def setUpClass(self):
        self.calc_1 = Calculations(10, 20)
        self.calc_2 = Calculations(-1, 1)
        self.calc_3 = Calculations(-1, -1)

    def test_sum(self) -> None:
        self.assertEqual(self.calc_1.get_sum(), 30, "The sum is wrong")
        self.assertEqual(self.calc_2.get_sum(), 0, "The sum is wrong")
        self.assertEqual(self.calc_3.get_sum(), -2, "The sum is wrong")

    def test_difference(self) ->None:
        self.assertEqual(self.calc_1.get_difference(), -10, "The difference is wrong")

    def test_product(self) -> None:
        self.assertEqual(self.calc_1.get_product(), 200, "The product is wrong")

    def test_quotient(self) -> None:
        self.assertEqual(self.calc_1.get_quotient(), 0.5, "The quotient is wrong")

        # self.assertRaises(ValueError, Calculations(1, 0).get_quotient)

        # Context Manager
        with self.assertRaises(ValueError):
            Calculations(1,0).get_quotient()


class TestAbsFunctions(unittest.TestCase):
    def test_positive_number(self) -> None:
        self.assertEqual(abs(10), 10)

    def test_negative_number(self) -> None:
        self.assertEqual(abs(-10), 10)

    def test_zero(self) -> None:
        self.assertEqual(abs(0), 0)


if __name__ == '__main__':
    unittest.main()

