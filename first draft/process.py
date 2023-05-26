import pandas as pd
import re

def process_meta_csv(chunk, erih_plus_dict):
    meta_data = chunk
    meta_data['venue'] = meta_data['venue'].astype(str)
    # Retrieve a list of publication venue's identifiers
    meta_data['issn_list'] = meta_data['venue'].str.findall(r' issn:(\d{4}-\d{3}[\dX])')
    # Retrieve the OMID identifier of the publication venue:
    meta_data['OC_omid'] = meta_data['venue'].str.extract(r':br/([^\s]*)')
    issn_series = meta_data['issn_list'].drop_duplicates()
    # Get the indexes of the relevant rows
    index_to_keep = issn_series.index
    OMID_series = meta_data['OC_omid'].loc[index_to_keep]

    # The lists must be converted to string to use them in the method .groupby
    meta_data['issn'] = meta_data['issn_list'].astype(str)
    # Extract the identifier (OMID) from the 'id' column
    meta_data['id'] = meta_data['id'].str.extract(r'(meta:[^\s]*)')
    meta_data = meta_data.groupby('issn', sort=False).agg({'id': lambda x: len(x.tolist()), 'issn': lambda x: x.iloc[0]})

    meta_data["OC_omid"] = OMID_series.values
    meta_data.rename(columns={'id': 'Publications_in_venue'}, inplace=True)
    # Initialize empty column for ERIH PLUS identifier
    meta_data['EP_id'] = ""
    lists = list(erih_plus_dict.keys())
    idx_set = set()
    for idx, row in meta_data["issn"].items():
        for el in row[1:-1].split(", "):
            el = el.replace("'", "")
            # el = a single issn identifier
            if el in lists:
                idx_set.add(idx)
                meta_data.at[idx, 'EP_id'] = erih_plus_dict[el]
    idx_list = list(idx_set)
    result = meta_data.loc[idx_list]
    
    result = result[['OC_omid', 'issn', 'EP_id', "Publications_in_venue"]]    
    return result


def process_file(input_file, erih_plus_dict):
    chunksize = 5 * 10 ** 3
    processed_chunks = []

    dataf = pd.DataFrame()

    # Read the input_file in chunks and process each chunk
    with pd.read_csv(input_file, chunksize=chunksize) as reader:
        for chunk in reader:
            processed_chunk = process_meta_csv(chunk, erih_plus_dict)
            processed_chunks.append(processed_chunk)
            dataf = pd.concat([dataf, processed_chunk], ignore_index = True)
    # Combine the processed chunks into a single DataFrame
    return dataf

def process_file_wrapper(args):
    input_file, erih_dict = args
    return input_file, process_file(input_file, erih_dict)

