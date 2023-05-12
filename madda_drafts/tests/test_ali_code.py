import unittest
import main
import process
import pandas as pd

# WHAT I WILL TRY TO TEST:
# for this function I will check 
# 1. that different types of issn are retrieved
# 2. if there are two issns they are both retrieved


class TestAliCode(unittest.TestCase):

    def setUp(self, input_data):
        self.input_data = pd.read_csv(input_data, 'r', encoding='utf-8') #path
        #add string check

    def tearDown(self, input_data):
        pass
        

    def test_process_meta_csv(self):





if __name__ == '__main__':
    unittest.main() # runs all of the tests just calling the file