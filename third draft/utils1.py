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


def getPercentage(num,den):
    return num * 100 / den


def get_erih_plus_dict(erih_df): #method to load dictionary

    erih_plus_dict = {}
    for idx, row in erih_df.iterrows():
        erih_plus_dict[row["Print ISSN"]] = row["Journal ID"]
        erih_plus_dict[row["Online ISSN"]] = row["Journal ID"]
    return erih_plus_dict


def process_doaj_file(doaj_df, merged_data):  #this is called in oc metaprocessor

    new_doaj = doaj_df.iloc[1:, [5, 6, 10]]

    # Create a dictionary of Open Access ISSNs
    open_access_dict = {}
    for index, row in new_doaj.iterrows():
        open_access_dict[row['Journal ISSN (print version)']] = True
        open_access_dict[row['Journal EISSN (online version)']] = True
    
    open_access_keys = list(open_access_dict.keys())

    merged_data['Open Access'] = "Uknown"
    for idx, row in merged_data["issn"].items():
        for el in row[1:-1].split(", "):
            el = el.replace("'", "")
            if el in open_access_keys:
                merged_data.at[idx, 'Open Access'] = True

    return merged_data