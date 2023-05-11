import pandas as pd
from utils import detect_delimiter

class CSV_to_DataFrame:
    def __init__(self, path):
        self.path = path

    def load_data(self):
        delimiter = detect_delimiter(self.path)
        df = pd.read_csv(self.path, delimiter=delimiter)
        return df

