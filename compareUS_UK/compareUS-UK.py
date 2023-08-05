import pandas as pd
import json
import csv
from OCMeta_coverage_classes import *
from Disciplines_Countries_classes import *
 
## to read files
def detect_delimiter(file_path):
    with open(file_path, 'r', newline='', encoding='utf-8') as file:
        dialect = csv.Sniffer().sniff(file.read(1024))
    return dialect.delimiter


def load_data(file_path):
        delimiter = detect_delimiter(file_path)
        df = pd.read_csv(file_path, delimiter=delimiter, encoding="UTF-8")
        return df

#####

erih = load_data("ds/ERIHPLUSapprovedJournals.csv") # should be result DF SSH_Publications in OCMeta?

with open("results/result_countries.json") as f:
    countries_j = json.load(f)

with open("results/result_disciplines.json") as f:
    disciplines_j = json.load(f)

# filtering for UK journals
uk_ID = list(countries_j["United Kingdom"])
uk_info = erih[erih["Journal ID"].isin(uk_ID)] # 1348 rows vs 1350 of results? but I have the previous ds here I think

# filtering for US journals
us_ID = list(countries_j["United States"])
us_info = erih[erih["Journal ID"].isin(us_ID)] # 1120 rows vs 1161 of results

# find how many disciplines

def count_disciplines_per_journal(df):
    count_list = []
    for idx, row in df.iterrows():
        disc = row["ERIH PLUS Disciplines"].split(", ")
        count_list.append(len(disc))

    df["disc_count"] = count_list

    return df

us = count_disciplines_per_journal(us_info)
uk = count_disciplines_per_journal(uk_info)

# need to find this in OCMeta? yes because need to find number of publication per journal

meta_processed = load_data("results/SSH_Publications_in_OC_Meta_and_Open_Access_status.csv")

us_meta = pd.merge(meta_processed, us, left_on="EP_id", right_on="Journal ID", how="inner") # keep only those present in meta
uk_meta = pd.merge(meta_processed, uk, left_on="EP_id", right_on="Journal ID", how="inner")

# to make the same df but only uk and us so we can use old code to process disciplines
meta_coverage_uk = uk_meta[["OC_omid", "issn", "EP_id", "Publications_in_venue", "Open Access"]]
meta_coverage_us = us_meta[["OC_omid", "issn", "EP_id", "Publications_in_venue", "Open Access"]]
#meta_coverage_uk.to_csv("compareUS_UK/meta_coverage_uk.csv", index=False) #they are now 1161 (??) wierd
#meta_coverage_us.to_csv("compareUS_UK/meta_coverage_us.csv", index=False) #and 1350

def counts(dictionary, label): 
        meta_coverage = meta_coverage_us
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
        count_df.to_csv("us_disciplines_count.csv", index=False)

        # since in this df a single publication and journal counts multiple times for each discipline,
        # how can we check that the result is correct?

        return count_df

print(counts(disciplines_j, "Disciplines"))


# making new ds with additional info --> filter columns
# us_meta = us_meta[["EP_id", "Publications_in_venue", "Original Title", "Country of Publication", "ERIH PLUS Disciplines", "disc_count"]]
# uk_meta = uk_meta[["EP_id", "Publications_in_venue", "Original Title", "Country of Publication", "ERIH PLUS Disciplines", "disc_count"]]

# us_meta.to_csv("compareUS_UK/us_data.csv", index=False) #they are now 1161 (??) wierd
# uk_meta.to_csv("compareUS_UK/uk_data.csv", index=False) #and 1350


'''
left to do:
4. integrate with old code --> this can be a class method maybe
5. re run everything with right datasets!!
'''


