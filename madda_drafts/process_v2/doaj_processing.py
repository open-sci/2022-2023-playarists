import pandas as pd

def process_doaj_file(merged_data):
    # Load DOAJ CSV file into a DataFrame
    doaj_file_path = 'journalcsv__doaj.csv'
    doaj_df = pd.read_csv(doaj_file_path, encoding="UTF-8")
    print(doaj_df[1:5])

    new_doaj = doaj_df.iloc[:, [5, 6, 10]] # it was row 1 but was skipping the first line

    # Create a dictionary of Open Access ISSNs
    open_access_dict = {}
    for index, row in new_doaj.iterrows():
        open_access_dict[row['Journal ISSN (print version)']] = True
        open_access_dict[row['Journal EISSN (online version)']] = True

    # Merge Open Access information with the main dataframe
    merged_data['Open Access'] = merged_data['OC_ISSN'].map(open_access_dict)
    # Fill missing Open Access information with 'Unknown'
    merged_data['Open Access'] = merged_data['Open Access'].fillna('Unknown')

    return merged_data
