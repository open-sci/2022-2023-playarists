import pandas as pd

def process_meta_csv(chunk, erih_plus_df):
    meta_data = chunk
    meta_data['venue'] = meta_data['venue'].astype(str)
    meta_data['issn'] = meta_data['venue'].str.extract(r'issn:(\d{4}-\d{3}[\dX])')
    # Extract the identifier (OMID) from the 'id' column
    meta_data['id'] = meta_data['id'].str.extract(r'(meta:[^\s]*)')

    lists = erih_plus_df['Print ISSN'].tolist()
    lists.extend((erih_plus_df['Online ISSN']).tolist())
    lists = list(filter(lambda x: not isinstance(x, float), lists))
    check = meta_data["issn"].isin(lists)
    idx_list = []
    for idx, row in check.items():
        if row == True:
            idx_list.append(idx)
    result = meta_data.loc[idx_list]
    
    merged_data_print = erih_plus_df.merge(result, left_on='Print ISSN', right_on='issn', how='inner')
    merged_data_online = erih_plus_df.merge(result, left_on='Online ISSN', right_on='issn', how='inner')
    merged_data = pd.concat([merged_data_print, merged_data_online], ignore_index=True)

    # Keep only the relevant columns for the mapping dataframe
    merged_data = merged_data[['id', 'issn', 'Journal ID', 'Print ISSN', 'Online ISSN']].rename(columns={'id': 'OC_OMID', 'issn': 'OC_ISSN', 'Journal ID': 'EP_ID', 'Print ISSN': 'EP_Print_ISSN', 'Online ISSN': 'EP_Online_ISSN'})

    # Create the 'EP_ISSN' column
    merged_data['EP_ISSN'] = merged_data['EP_Print_ISSN'].combine_first(merged_data['EP_Online_ISSN'])
    
    # Drop the 'EP_Print_ISSN' and 'EP_Online_ISSN' columns
    merged_data = merged_data.drop(columns=['EP_Print_ISSN', 'EP_Online_ISSN'])

    # Drop rows with NaN values in the 'OC_ISSN' column
    merged_data = merged_data.dropna(subset=['OC_ISSN']).reset_index(drop=True)
    
    return merged_data


def process_file(input_file, erih_plus_df):
    chunksize = 5 * 10 ** 3
    dataf = pd.DataFrame()

    # Read the input_file in chunks and process each chunk
    with pd.read_csv(input_file, chunksize=chunksize) as reader:
        for chunk in reader:
            processed_chunk = process_meta_csv(chunk, erih_plus_df)
            dataf = pd.concat([dataf, processed_chunk], ignore_index = True)

    # Group by 'EP_ID' and aggregate 'OC_ISSN' into a list, keep 'OC_OMID' and 'EP_ISSN' from the first row of each group
    dataf = dataf.groupby('EP_ID').agg({'OC_ISSN': list, 'OC_OMID': 'first', 'EP_ISSN': 'first'}).reset_index()

    # Add 'Publication_in_venue' column
    dataf['Publication_in_venue'] = dataf['OC_ISSN'].apply(len)

    return dataf


def process_file_wrapper(args):
    input_file, erih_plus_df = args
    return input_file, process_file(input_file, erih_plus_df)

