import pandas as pd

class DOAJProcessor:
    def __init__(self, doaj_file_path):
        self.doaj_file_path = doaj_file_path

    def process_doaj_file(self, merged_data):
        # Load DOAJ CSV file into a DataFrame
        doaj_df = pd.read_csv(self.doaj_file_path, encoding="UTF-8")

        new_doaj = doaj_df.iloc[1:, [5, 6, 10]]

        # Create a dictionary of Open Access ISSNs
        open_access_dict = {}
        for index, row in new_doaj.iterrows():
            open_access_dict[row['Journal ISSN (print version)']] = True
            open_access_dict[row['Journal EISSN (online version)']] = True

        # Merge Open Access information with the main dataframe
        merged_data['Open Access'] = merged_data['issn'].map(open_access_dict)
        # Fill missing Open Access information with 'Unknown'
        merged_data['Open Access'] = merged_data['Open Access'].fillna('Unknown')

        return merged_data
