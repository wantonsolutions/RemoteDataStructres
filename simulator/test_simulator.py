import unittest
import cuckoo
import hash

class Test_Table(unittest.TestCase):

    def test_zero_index(self):
        self.assertEqual(0, hash.get_table_id_from_index(0))

    def test_1_index(self):
        self.assertEqual(1, hash.get_table_id_from_index(1))

    def test_2_index(self):
        self.assertEqual(0, hash.get_table_id_from_index(2))

class Test_Heuristic(unittest.TestCase):

    def test_zero_distance_table(self):
        current_index = 0
        target_index = 0
        table_size = 8
        distance = cuckoo.heuristic_3(current_index, target_index, table_size)
        self.assertEqual(0, distance)

    #todo test out heuristic_3 some more


if __name__ == '__main__':
    unittest.main()