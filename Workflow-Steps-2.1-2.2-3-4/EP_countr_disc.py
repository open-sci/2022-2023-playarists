#answer to question 2 and 3 of research: 
#2. What are the disciplines that have more publications?
#3. What are countries providing the largest number of publications and journals? 
import pandas as pd
from pprint import pprint
from retrieve_doaj_country import retrieve_doaj_country

result_df = pd.read_csv("Workflow-Steps-1.1-1.2/resultDf.csv")
erih_df = pd.read_csv("Workflow-Steps-1.1-1.2/ERIHPLUSapprovedJournals.csv", sep= ";")
merged_df = pd.merge(erih_df, result_df, left_on='Journal ID', right_on='EP_id')
doaj_df = pd.read_csv("Workflow-Steps-1.1-1.2\journalcsv__doaj.csv")
doaj_df = doaj_df[["Journal ISSN (print version)", "Journal EISSN (online version)", "Country of publisher"]]

# step 2.2 and 2.3 of workflow
def create_dict(disc_dict, countr_dict, df): #df is erih-plus but merged with resultDf
    empty_countries = []
    for idx, row in df.iterrows():

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
             
        #countries. NEED TO UPDATE BECAUSE THERE ARE MULTIPLE COUNTRIES SOMETIMES
        if pd.isna(row["Country of Publication"]):
            empty_countries.append(row["Journal ID"])

            # create subdataframe with only rows of unmerged data that are unmatched
            # WRITE A FUNCTION TO SEE IF EMPTY_COUNTRIES CAN BE FOUND IN DOAJ AND CALL IT 
            # for the moment I am saving in a list
            
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
    #print(empty_countries)
    #print("\n________________________")
    #pprint(df)

    doaj_country_dict = retrieve_doaj_country(empty_countries, df, doaj_df, countr_dict)

    return disc_dict, doaj_country_dict, empty_countries 



disc_dict, country_dict, empty_countries = create_dict({}, {}, merged_df)

#print(len(countr_dict)) #114
#print(len(empty_countries)) #121

# step 3 and 4 of workflow actually condensed
def counts(result_df, country_dict, disc_dict):
    result_df = result_df[["EP_id", "Publications_in_venue"]]
    disciplines_count = pd.DataFrame(columns=['Discipline','Journal_count','Publication_count'])
    countries_count = pd.DataFrame(columns=['Country','Journal_count','Publication_count'])

    for key, value in disc_dict.items():
        venue_count = len(value) # 1592 venues for Interdisciplinary research in the Social Sciences
        #print(venue_count)
        venue_in_OCMeta_count =  result_df[result_df["EP_id"].isin(value)]
        venue_in_OCMeta_count.reset_index(drop=True, inplace=True)
        venue_per_disc = len(venue_in_OCMeta_count) # 1592
        #print(venue_per_disc)
        pub_per_disc = venue_in_OCMeta_count['Publications_in_venue'].sum() #753265
        
        #create df row
        disc_row = pd.DataFrame([{'Discipline' : key ,'Journal_count' : venue_per_disc , 'Publication_count' : pub_per_disc}])
        disciplines_count = pd.concat([disciplines_count, disc_row], ignore_index = True)
    
    for key, value in country_dict.items():
        venue_in_OCMeta_count =  result_df[result_df["EP_id"].isin(value)]
        venue_in_OCMeta_count.reset_index(drop=True, inplace=True)
        venue_per_country = len(venue_in_OCMeta_count) 
        pub_per_country = venue_in_OCMeta_count['Publications_in_venue'].sum() 
        
        #create df row
        country_row = pd.DataFrame([{'Country' : key ,'Journal_count' : venue_per_country , 'Publication_count' : pub_per_country}])
        countries_count = pd.concat([countries_count, country_row], ignore_index = True)
    
    return disciplines_count, countries_count

''' for el in pub_count["EP_id"].tolist():
            value.remove(el)
        print(value)   
        print(len(value))''' #here I wanted to double check that the number of unmatched value ep_id
#was complementare to the number of the matched one but it is telling me at one point that "el is not in values".
#this shouldn't be possible, given that the df should be a subsection of value list!
#something is up... it happens with EP_id 498742


disciplines_count, countries_count = counts(result_df, country_dict, disc_dict)
#pprint(disciplines_count)
#print("___________________________________________________")
#pprint(countries_count)

disciplines_count.to_csv('Workflow-Steps-2.1-2.2-3-4\disciplinesnew_count.csv', index=False)
countries_count.to_csv('Workflow-Steps-2.1-2.2-3-4\countriesnew_count.csv', index=False)


