import pandas as pd

def map_issn_list_to_open_access(issn_list, open_access_dict):
    return [open_access_dict.get(issn, 'Unknown') for issn in issn_list]

def process_doaj_file(merged_data, doaj_file_path):
    # Load DOAJ CSV file into a DataFrame
    doaj_df = pd.read_csv(doaj_file_path, encoding="UTF-8")

    new_doaj = doaj_df.iloc[1:, [5, 6, 10]]

    # Create a dictionary of Open Access ISSNs
    open_access_dict = {}
    for index, row in new_doaj.iterrows():
        open_access_dict[row['Journal ISSN (print version)']] = True
        open_access_dict[row['Journal EISSN (online version)']] = True

    # Flatten 'OC_ISSN' list of lists into a single list
    merged_data['OC_ISSN'] = merged_data['OC_ISSN'].apply(lambda x: [item for sublist in x for item in sublist])

    # Merge Open Access information with the main dataframe
    merged_data['Open Access'] = merged_data['OC_ISSN'].apply(lambda x: map_issn_list_to_open_access(x, open_access_dict))

    # calculate 'Publication_in_venue' as the length of the list (counting duplicates)
    merged_data['Publication_in_venue'] = merged_data['OC_ISSN'].apply(len)

    # convert 'OC_ISSN' lists to sets to remove duplicates
    merged_data['OC_ISSN'] = merged_data['OC_ISSN'].apply(set)

    # Create another DataFrame with unique 'OC_ISSN' and 'Publication_in_venue' columns
    #unique_ISSN_df = merged_data.explode('OC_ISSN')[['OC_ISSN', 'Publication_in_venue']].drop_duplicates()

    # Keep only the required columns in the output
    #merged_data = merged_data[['OC_OMID', 'OC_ISSN', 'EP_ID', 'EP_ISSN', 'Publication_in_venue', 'Open Access']]
    merged_data = merged_data[['OC_OMID', 'OC_ISSN', 'EP_ID', 'Publication_in_venue', 'Open Access']]


    return merged_data     
