import pandas as pd

def process_doaj_file(merged_data, doaj_file_path):
    # Load DOAJ CSV file into a DataFrame
    doaj_df = pd.read_csv(doaj_file_path, encoding="UTF-8")

    new_doaj = doaj_df.iloc[1:, [5, 6, 10]]

    # Create a dictionary of Open Access ISSNs
    open_access_dict = {}
    for index, row in new_doaj.iterrows():
        open_access_dict[row['Journal ISSN (print version)']] = True
        open_access_dict[row['Journal EISSN (online version)']] = True

    # Merge Open Access information with the main dataframe
    merged_data['Open Access'] = merged_data['OC_ISSN'].map(open_access_dict)
    # Fill missing Open Access information with 'Unknown'
    merged_data['Open Access'] = merged_data['Open Access'].fillna('Unknown')
    
    # Add 'Publication_in_venue' column
    merged_data['Publication_in_venue'] = merged_data.groupby('EP_ID')['EP_ID'].transform('count')
    
    # Create another DataFrame with unique 'OC_ISSN' and 'Publication_in_venue' columns
    unique_ISSN_df = merged_data[['OC_ISSN', 'Publication_in_venue']].drop_duplicates()

    # Keep only the required columns in the output
    merged_data = merged_data[['OC_OMID', 'OC_ISSN', 'EP_ID', 'EP_ISSN', 'Publication_in_venue', 'Open Access']]
    #merged_data = merged_data[['OC_OMID', 'OC_ISSN', 'EP_ID', 'EP_ISSN', 'Publication Count', 'Publication_in_venue', 'Open Access']]
    

    return merged_data, unique_ISSN_df
