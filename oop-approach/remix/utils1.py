import csv
import pandas as pd

def detect_delimiter(file_path):
    with open(file_path, 'r', newline='', encoding='utf-8') as file:
        dialect = csv.Sniffer().sniff(file.read(1024))
    return dialect.delimiter

def load_data(file_path):
        delimiter = detect_delimiter(file_path)
        df = pd.read_csv(file_path, delimiter=delimiter, encoding="UTF-8")
        return df