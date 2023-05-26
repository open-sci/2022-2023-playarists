import pandas as pd
import re
from csv_to_dataframe import CSV_to_DataFrame


class ProcessMetaCSV:
    def __init__(self, erih_plus_dict):
        self.erih_plus_dict = erih_plus_dict

    def process_meta_csv(self, chunk):
        meta_data = chunk
        meta_data['venue'] = meta_data['venue'].astype(str)
        meta_data['issn_list'] = meta_data['venue'].str.findall(r' issn:(\d{4}-\d{3}[\dX])')
        meta_data['OC_omid'] = meta_data['venue'].str.extract(r':br/([^\s]*)')
        issn_series = meta_data['issn_list'].drop_duplicates()
        index_to_keep = issn_series.index
        OMID_series = meta_data['OC_omid'].loc[index_to_keep]

        meta_data['issn'] = meta_data['issn_list'].astype(str)
        meta_data['id'] = meta_data['id'].str.extract(r'(meta:[^\s]*)')
        meta_data = meta_data.groupby('issn', sort=False).agg({'id': lambda x: len(x.tolist()), 'issn': lambda x: x.iloc[0]})

        meta_data['OC_omid'] = OMID_series.values
        meta_data.rename(columns={'id': 'Publications_in_venue'}, inplace=True)
        meta_data['EP_id'] = ""
        lists = list(self.erih_plus_dict.keys())
        idx_set = set()
        for idx, row in meta_data["issn"].items():
            for el in row[1:-1].split(", "):
                el = el.replace("'", "")
                if el in lists:
                    idx_set.add(idx)
                    meta_data.at[idx, 'EP_id'] = self.erih_plus_dict[el]
        idx_list = list(idx_set)
        result = meta_data.loc[idx_list]

        result = result[['OC_omid', 'issn', 'EP_id', "Publications_in_venue"]]
        return result

    def process_file(self, input_file):
        chunksize = 5 * 10 ** 3
        processed_chunks = []
        dataf = pd.DataFrame()

        with pd.read_csv(input_file, chunksize=chunksize) as reader:
            for chunk in reader:
                processed_chunk = self.process_meta_csv(chunk)
                processed_chunks.append(processed_chunk)
                dataf = pd.concat([dataf, processed_chunk], ignore_index=True)

        return dataf

    def process_file_wrapper(self, args): #args potrebbe essere semplicemente input_file
        input_file = args
        return input_file, self.process_file(input_file) #sono i batch_files

    def get_erih_plus_dict(self, path):
        csv_to_dataframe = CSV_to_DataFrame(path)
        df = csv_to_dataframe.load_data()
        erih_plus_dict = {}
        for idx, row in df.iterrows():
            erih_plus_dict[row["Print ISSN"]] = row["Journal ID"]
            erih_plus_dict[row["Online ISSN"]] = row["Journal ID"]
        return erih_plus_dict