import pandas as pd
from csv_to_dataframe import CSV_to_DataFrame
from utils import detect_delimiter

class ERIHPlusProcessor:
    def __init__(self, erih_plus_path, doaj_path):
        self.erih_plus_path = erih_plus_path
        self.doaj_path = doaj_path

    def load_erih_plus_data(self):
        csv_to_dataframe = CSV_to_DataFrame(self.erih_plus_path)
        df = csv_to_dataframe.load_data()
        return df
    
    def process_disciplines(self):
        erih_plus_df = self.load_erih_plus_data()
        result_df = pd.read_csv('resultDf.csv')  # load the resultDf.csv

        # Merge the dataframes on the common identifier
        merged_df = pd.merge(erih_plus_df, result_df, left_on='Journal ID', right_on='EP_id')

        disciplines = merged_df['ERIH PLUS Disciplines'].str.split(';').explode()
        discipline_counts = disciplines.value_counts().reset_index()
        discipline_counts.columns = ['Discipline', 'Journal_count']

        # Use 'Publications_in_venue' from the merged dataframe
        discipline_counts['Publication_count'] = discipline_counts['Discipline'].map(merged_df.groupby('ERIH PLUS Disciplines')['Publications_in_venue'].sum())
        
        return discipline_counts
    
    def process_countries(self):
        erih_plus_df = self.load_erih_plus_data()
        result_df = pd.read_csv('resultDf.csv')  # load the resultDf.csv

        # Merge the dataframes on the common identifier
        merged_df = pd.merge(erih_plus_df, result_df, left_on='Journal ID', right_on='EP_id')

        country_counts = merged_df['Country of Publication'].value_counts().reset_index()
        country_counts.columns = ['Country', 'Journal_count']

        # Use 'Publications_in_venue' from the merged dataframe
        country_counts['Publication_count'] = country_counts['Country'].map(merged_df.groupby('Country of Publication')['Publications_in_venue'].sum())
        
        return country_counts
