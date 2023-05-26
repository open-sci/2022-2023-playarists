import pandas as pd
from retrieve_doaj_country import retrieve_doaj_country

# ==================== PROCESSORS FOR RETRIEVING DISCIPLINES ======================= #

class ResultsProcessor(object):
    def __init__(self, meta_coverage): # takes in input a PlayaristProcessor object
        self.meta_coverage = meta_coverage.process_files()
        self.erih_df = meta_coverage.get_erih_df()
        self.doaj_df = meta_coverage.get_doaj_df()     


class CountriesProcessor(ResultsProcessor):
    def __init__(self, meta_coverage):  
        self.doaj_df = self.doaj_df[["Journal ISSN (print version)", "Journal EISSN (online version)", "Country of publisher"]] 
        self.unmatched_countries = []
    

        super().__init__(meta_coverage)

    def create_countries_dict(self):
        countr_dict = {}
       
        merged_df = pd.merge(self.erih_df, self.meta_coverage, left_on='Journal ID', right_on='EP_id')

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

        complete_country_dict = retrieve_doaj_country(self.unmatched_countries, self.merged_df, self.doaj_df, countr_dict)
        return complete_country_dict # a tuple con (countr_dict, unmatched_df)                


class DisciplinesProcessor(ResultsProcessor):
    def __init__(self, meta_coverage):
        super().__init__(meta_coverage)


    def create_disciplines_dict(self): 
        disc_dict = {}

        merged_df = pd.merge(self.erih_df, self.meta_coverage, left_on='Journal ID', right_on='EP_id')

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

    def __init__(self, meta_coverage, export_path): #dir_path where to export csv
        self.export_path = export_path
        
        super().__init__(meta_coverage)


    def counts(self, dictionary, label): #dictionary is a DisciplinesCountriesProcessor object
        meta_coverage = self.meta_coverage[["EP_id", "Publications_in_venue"]]
        count_df = pd.DataFrame(columns=[str(label),'Journal_count','Publication_count'])
    
        for key, value in dictionary.items():
         
            venue_in_OCMeta_count =  meta_coverage[meta_coverage["EP_id"].isin(value)]
            venue_in_OCMeta_count.reset_index(drop=True, inplace=True)
            venue_per_variable = len(venue_in_OCMeta_count) # 1592
            #print(venue_per_disc)
            pub_per_variable = venue_in_OCMeta_count['Publications_in_venue'].sum() #753265
            
            #create df row
            new_row = pd.DataFrame([{str(label) : key ,'Journal_count' : venue_per_variable , 'Publication_count' : pub_per_variable}])
            count_df = pd.concat([count_df, new_row], ignore_index = True)
        
        count_df = count_df.sort_values('Publication_count', ascending=False)        
        count_df.to_csv(self.export_path, index=False)

        return count_df
