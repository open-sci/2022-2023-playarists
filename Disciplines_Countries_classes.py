import pandas as pd
from retrieve_doaj_country import retrieve_doaj_country
import json
from utils import load_data

# ==================== PROCESSORS FOR RETRIEVING DISCIPLINES ======================= #

class ResultsProcessor(object):
    def __init__(self, meta_coverage, remove_megajournals, meta_coverage_processed_files): # takes in input a PlayaristProcessor object
        self.meta_coverage = meta_coverage
        self.meta_df = pd.read_csv(meta_coverage_processed_files).iloc[4:] if remove_megajournals else pd.read_csv(meta_coverage_processed_files)
        self.erih_df = meta_coverage.get_erih_df()
        self.doaj_df = meta_coverage.get_doaj_df()   
        self.remove_megajournals = remove_megajournals
        



class CountriesProcessor(ResultsProcessor):
    def __init__(self, meta_coverage, remove_megajournals=False, meta_coverage_processed_files="SSH_Publications_in_OC_Meta_and_Open_Access_status.csv"): 
        super().__init__(meta_coverage, remove_megajournals, meta_coverage_processed_files)
        self.doaj_df = self.doaj_df[["Journal ISSN (print version)", "Journal EISSN (online version)", "Country of publisher"]] 
        self.unmatched_countries = []

    def create_countries_dict(self):
        countr_dict = {}
        merged_df = pd.merge(self.erih_df, self.meta_df, left_on='Journal ID', right_on='EP_id')
        for idx, row in merged_df.iterrows():
            if pd.isna(row["Country of Publication"]):
                self.unmatched_countries.append(row["Journal ID"])

            else:
                if len(countr_dict) == 0:
                    countries = row["Country of Publication"].split(', ')
                    countr_dict= {key: [row["Journal ID"]] for key in countries}

                else: 
                    countries = set(row["Country of Publication"].split(', '))
                    keys = set(countr_dict.keys())
                    diff = countries - keys

                    if len(diff) == 0:
                        for key in countries:
                            countr_dict[key].append(row["Journal ID"])
                    else:
                        countr_dict.update({key: [] for key in diff})            
                        for key in countries:
                            countr_dict[key].append(row["Journal ID"])

        complete_country_dict = retrieve_doaj_country(self.unmatched_countries, merged_df, self.doaj_df, countr_dict)
        return complete_country_dict # a tuple (countr_dict, unmatched_df)                


class DisciplinesProcessor(ResultsProcessor):
    def __init__(self, meta_coverage, remove_megajournals=False, meta_coverage_processed_files="SSH_Publications_in_OC_Meta_and_Open_Access_status.csv"):
        super().__init__(meta_coverage, remove_megajournals, meta_coverage_processed_files)


    def create_disciplines_dict(self): 
        disc_dict = {}
        merged_df = pd.merge(self.erih_df, self.meta_df, left_on='Journal ID', right_on='EP_id')

        for idx, row in merged_df.iterrows():
         #disciplines
            if len(disc_dict) == 0:
                disciplines = row["ERIH PLUS Disciplines"].split(', ')
                disc_dict= {key: [row["Journal ID"]] for key in disciplines}
            
            else: 
                disciplines = set(row["ERIH PLUS Disciplines"].split(', '))
                keys = set(disc_dict.keys())
                diff = disciplines - keys

                if len(diff) == 0:
                    for key in disciplines:
                        disc_dict[key].append(row["Journal ID"])
                else:
                    disc_dict.update({key: [] for key in diff})            
                    for key in disciplines:
                        disc_dict[key].append(row["Journal ID"])

        return disc_dict
    
    
# ================= COUNTS AND CSV EXPORT ====================== #
    
class CountsProcessor(ResultsProcessor):

    def __init__(self, meta_df, export_path, remove_megajournals=False, meta_coverage_processed_files="SSH_Publications_in_OC_Meta_and_Open_Access_status.csv"):
        self.export_path = export_path
        super().__init__(meta_df, remove_megajournals, meta_coverage_processed_files)


    def counts(self, dictionary, label): #dictionary is a DisciplinesCountriesProcessor object
        meta_coverage = self.meta_df[["EP_id", "Publications_in_venue"]]
        count_df = pd.DataFrame(columns=[str(label),'Journal_count','Publication_count'])
    
        for key, value in dictionary.items():
         
            venue_in_OCMeta_count =  meta_coverage[meta_coverage["EP_id"].isin(value)]
            venue_in_OCMeta_count.reset_index(drop=True, inplace=True)
            venue_per_variable = len(venue_in_OCMeta_count)

            pub_per_variable = venue_in_OCMeta_count['Publications_in_venue'].sum()
            
            #create df row
            new_row = pd.DataFrame([{str(label) : key ,'Journal_count' : venue_per_variable , 'Publication_count' : pub_per_variable}])
            count_df = pd.concat([count_df, new_row], ignore_index = True)
        
        count_df = count_df.sort_values('Publication_count', ascending=False)        
        count_df.to_csv(self.export_path, index=False)

        return count_df

# ==================== US-EU comparison ====================== #

class Compare_US_EU(ResultsProcessor):
    def __init__(self, meta_coverage, remove_megajournals=False,  meta_coverage_processed_files="SSH_Publications_in_OC_Meta_and_Open_Access_status.csv"): 
        super().__init__(meta_coverage, remove_megajournals, meta_coverage_processed_files)
   
    
    def compare_us_eu(self, erih_ds, countries_dict):
        # loading data 
        erih = load_data(erih_ds)
        countries_j = countries_dict

        #with open(countries_dict) as f:
            #countries_j = json.load(f)
        #with open(disciplines_dict) as f:
            #disciplines_j = json.load(f)

        # filtering for EU journals
        eu_ID = []
        eu_contries = ["Spain", "Romania", "Sweden", "United Kingdom", "Bulgaria", "Italy", "France", "Belgium", "Greece", "Poland", "Slovenia", "Albania", "Czechia", "Serbia", "Ukraine", "Netherlands", "Hungary", "Germany", "Italy, Norway", "Portugal", "Estonia", "Norway", "Bosnia and Herzegovina", "Slovakia", "Macedonia", "Lithuania", "Denmark", "Latvia", "Switzerland", "Finland", "United Kingdom", "Montenegro", "Ireland", "Croatia", "Austria", "Moldova", "Cyprus", "Germany, United Kingdom", "Czechia, Finland", "United Kingdom, Netherlands", "Luxembourg", "Iceland", "United Kingdom, Norway", "Malta", "Switzerland, United Kingdom", "Spain, United Kingdom", "Germany, Italy", "Belarus"]
        for country in countries_j:
            if country in eu_contries:
                eu_ID.extend(list(countries_j[country]))


        eu_info = erih[erih["Journal ID"].isin(eu_ID)]

        # filtering for US journals
        us_ID = list(countries_j["United States"])
        us_info = erih[erih["Journal ID"].isin(us_ID)] # 1120 rows vs 1161 of results

        # find how many disciplines add column to erih plus filtered

        def count_disciplines_per_journal(df):
            count_list = []
            for idx, row in df.iterrows():
                disc = row["ERIH PLUS Disciplines"].split(", ")
                count_list.append(len(disc))

            df["disc_count"] = count_list

            return df

        us = count_disciplines_per_journal(us_info)
        eu = count_disciplines_per_journal(eu_info)


        # now filter SSH_Publications_in_OC_Meta_and_Open_Access_status.csv to eu and us erih info
        meta_processed = self.meta_df

        us_meta = pd.merge(meta_processed, us, left_on="EP_id", right_on="Journal ID", how="inner") # keep only those present in meta
        eu_meta = pd.merge(meta_processed, eu, left_on="EP_id", right_on="Journal ID", how="inner")

        # DATASET US_DATA and EU_DATA 
        us_data = us_meta[["EP_id", "Publications_in_venue", "Original Title", "Country of Publication", "ERIH PLUS Disciplines", "disc_count"]]
        us_data = us_data.rename(columns={"Original Title": "Original_Title", "Country of Publication": "Country_of_Publication", "ERIH PLUS Disciplines" : "ERIH_PLUS_Disciplines"})
        eu_data = eu_meta[["EP_id", "Publications_in_venue", "Original Title", "Country of Publication", "ERIH PLUS Disciplines", "disc_count"]]
        eu_data = eu_data.rename(columns={"Original Title": "Original_Title", "Country of Publication": "Country_of_Publication", "ERIH PLUS Disciplines" : "ERIH_PLUS_Disciplines"})
       
        us_data.to_csv("compareUS_EU/us_data.csv", index=False) #they are now 1161 (??) wierd
        eu_data.to_csv("compareUS_EU/eu_data.csv", index=False) #and 1350

        # DATASET META_COVERAGE_EU and META_COVERAGE_US
        meta_coverage_eu = eu_meta[["OC_omid", "issn", "EP_id", "Publications_in_venue", "Open Access"]]
        meta_coverage_us = us_meta[["OC_omid", "issn", "EP_id", "Publications_in_venue", "Open Access"]]
        meta_coverage_eu.to_csv("compareUS_EU/meta_coverage_eu.csv", index=False)
        meta_coverage_us.to_csv("compareUS_EU/meta_coverage_us.csv", index=False) 

        return meta_coverage_us, meta_coverage_eu, us_data, eu_data


    # RESULT DATASET WITH PUBLICATIONS AND JOURNALS PER DISCIPLINE

    def counts_us_eu(self, disciplines_dict, label, meta_coverage_us_or_eu, export_path): 
        meta_coverage = meta_coverage_us_or_eu
        count_df = pd.DataFrame(columns=[str(label),'Journal_count','Publication_count'])
    
        for key, value in disciplines_dict.items():
        
            venue_in_OCMeta_count =  meta_coverage[meta_coverage["EP_id"].isin(value)]
            venue_in_OCMeta_count.reset_index(drop=True, inplace=True)
            venue_per_variable = len(venue_in_OCMeta_count)

            pub_per_variable = venue_in_OCMeta_count['Publications_in_venue'].sum()
            
            #create df row
            new_row = pd.DataFrame([{str(label) : key ,'Journal_count' : venue_per_variable , 'Publication_count' : pub_per_variable}])
            count_df = pd.concat([count_df, new_row], ignore_index = True)
        
        count_df = count_df.sort_values('Publication_count', ascending=False)        
        count_df.to_csv(export_path, index=False)

        # since in this df a single publication and journal counts multiple times for each discipline,
        # how can we check that the result is correct?

        return count_df
