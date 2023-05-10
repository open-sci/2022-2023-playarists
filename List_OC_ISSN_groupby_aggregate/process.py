import pandas as pd
import re
#from IPython.display import display

def process_meta_csv(chunk, erih_plus_df):
    meta_data = chunk
    meta_data['venue'] = meta_data['venue'].astype(str)
    meta_data = meta_data.dropna(subset=['venue'])  
    meta_data['venue'] = meta_data['venue'].str.strip()

    meta_data['issn'] = meta_data['venue'].str.findall(r'issn:(\d{4}-\d{3}[\dX])')
    
    meta_data['issn'] = meta_data['issn'].apply(lambda x: ','.join(x) if len(x) > 0 else '')
    
    meta_data['issn'] = meta_data['issn'].str.split(',')

    meta_data['OC_OMID'] = meta_data['id'].str.extract(r'(br/([^\s]*))')[1]

    lists = erih_plus_df['Print ISSN'].tolist()
    lists.extend((erih_plus_df['Online ISSN']).tolist())
    lists = list(filter(lambda x: not isinstance(x, float), lists))
    check = meta_data["issn"].apply(lambda x: any(item for item in x if item in lists))
    idx_list = [idx for idx, row in check.items() if row]
    result = meta_data.loc[idx_list]
    
    merged_data_print = erih_plus_df.merge(result.explode('issn'), left_on='Print ISSN', right_on='issn', how='inner')
    merged_data_online = erih_plus_df.merge(result.explode('issn'), left_on='Online ISSN', right_on='issn', how='inner')
    merged_data = pd.concat([merged_data_print, merged_data_online], ignore_index=True)

    
    merged_data = merged_data[['OC_OMID', 'issn', 'Journal ID', 'Print ISSN', 'Online ISSN']].rename(columns={'issn': 'OC_ISSN', 'Journal ID': 'EP_ID', 'Print ISSN': 'EP_Print_ISSN', 'Online ISSN': 'EP_Online_ISSN'})


    merged_data['EP_ISSN'] = merged_data['EP_Print_ISSN'].combine_first(merged_data['EP_Online_ISSN'])
    merged_data = merged_data.drop(columns=['EP_Print_ISSN', 'EP_Online_ISSN'])
    merged_data = merged_data.dropna(subset=['OC_ISSN']).reset_index(drop=True)

    merged_data = merged_data.groupby(['EP_ID', 'OC_OMID']).agg({'OC_ISSN': list}).reset_index()

    
    return merged_data


def process_file(input_file, erih_plus_df):
    chunksize = 5 * 10 ** 3
    dataf = pd.DataFrame()

    # Read the input_file in chunks and process each chunk
    with pd.read_csv(input_file, chunksize=chunksize) as reader:
        for chunk in reader:
            processed_chunk = process_meta_csv(chunk, erih_plus_df)
            dataf = pd.concat([dataf, processed_chunk], ignore_index = True)

    
    #dataf = dataf.groupby(['EP_ID', 'OC_OMID']).agg({'OC_ISSN': list, 'EP_ISSN': lambda x: list(set(x))}).reset_index()
    dataf = dataf.groupby(['EP_ID', 'OC_OMID']).agg({'OC_ISSN': list}).reset_index()


    # Add 'Publication_in_venue' column
    dataf['Publication_in_venue'] = dataf['OC_ISSN'].apply(len)

    return dataf


def process_file_wrapper(args):
    input_file, erih_plus_df = args
    return input_file, process_file(input_file, erih_plus_df)

