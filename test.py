import subprocess
import pandas as pd
import glob
import os
import re
import pandas as pd

# Define the path to the Python file you want to execute
python_file = 'run_workflow.py'
erih_plus = "ERIHPLUSapprovedJournals.csv"
doaj = "journalcsv__doaj.csv"


def create_test_sets(length, start=0):
    # create erih_plus_test
    erih_plus_df = pd.read_csv(erih_plus, delimiter=";")
    total_length = erih_plus_df.shape[0]
    if length + start < total_length:
        erih_test = erih_plus_df.iloc[start:length]
    elif start < total_length:
        erih_test = erih_plus_df.iloc[start:total_length]
    else:
        print("You parameters are not accettable")
        return None
    erih_test.to_csv('erih_test.csv')

    print_issn, online_issn = [], []
    for idx, el in erih_test["Print ISSN"].items():
        if type(el) == type(str()):
            print_issn.append(el)
    for idx, el in erih_test["Online ISSN"].items():
        if type(el) == type(str()):
            online_issn.append(el)

    # create doaj_test according to erih_plus_text
    doaj_df = pd.read_csv(doaj)
    doaj_idx = []
    doaj_issn = []
    for idx, row in doaj_df.iterrows():
        if row["Journal ISSN (print version)"] in print_issn and type(row["Journal ISSN (print version)"]) == type(str()):
            doaj_idx.append(idx)
            doaj_issn.append(row["Journal ISSN (print version)"])
        elif row["Journal EISSN (online version)"] in online_issn and type(row["Journal EISSN (online version)"]) == type(str()):
            doaj_idx.append(idx)
            doaj_issn.append(row["Journal EISSN (online version)"])

    additional_idx = []
    for n in range(len(doaj_idx)-1):
        if doaj_idx[n+1] != doaj_idx[n] + 1 and len(additional_idx) < 15:
            additional_idx.append(doaj_idx[n] + 1)
    doaj_idx = doaj_idx + additional_idx

    doaj_test = doaj_df.iloc[doaj_idx]
    doaj_test = doaj_test.drop(doaj_test.columns[0], axis=1)
    doaj_test.to_csv('doaj_test.csv')

    # csv_dump
    csv = ["id", "title", "author", "issue", "volume", "venue", "page", "pub_date", "type", "publisher", "editor"]
    rows_erih_test = erih_test.shape[0]
    publication_base = "doi:test/100001."
    issue_base = "issn:0000-00"
    issue_number = 10
    publication_number = 0
    internal_id_pub = "meta:br/101010"
    internal_id_issue = "meta:br/202020"
    expected_pub = 1

    oc_meta_coverage_prep = []
    country_dict, disciplines_dict = {}, {}

    x = 0
    while x+1 < rows_erih_test:
        path = "fake_OC_Meta/fake_csv" + str(x//2) +".csv"
        sub_dicts = []
        sub_df = erih_test[x:x+2]
        for idx, row in sub_df.iterrows():
            y = 0
            if publication_number % 2 == 1:
                sub_dict = {key: "" for key in csv}
                sub_dict["id"] = internal_id_pub+str(publication_number)+" "+publication_base+str(publication_number)
                sub_dict["venue"] = "["+internal_id_issue+str(issue_number)+" "+issue_base+str(issue_number)+"]"
                sub_dicts.append(sub_dict) 
                publication_number+=1
            else:
                sub_dict = {key: "" for key in csv}
                sub_dict["id"] = internal_id_issue+str(issue_number)+" "+issue_base+str(issue_number)
                sub_dicts.append(sub_dict)

            oc_meta_coverage_dict = {}
            oc_meta_coverage_dict["OC_omid"] = "202020"+str(issue_number)

            if row["Country of Publication"] not in country_dict:
                country_dict[row["Country of Publication"]] = {
                    "Journal_count": 1,
                    "Publication_count": expected_pub,
                }
            else:
                country_dict[row["Country of Publication"]]["Journal_count"] = country_dict[row["Country of Publication"]]["Journal_count"] + 1
                country_dict[row["Country of Publication"]]["Publication_count"] = country_dict[row["Country of Publication"]]["Publication_count"] + expected_pub

            for el in row["ERIH PLUS Disciplines"].split(', '):
                if el in disciplines_dict:
                    disciplines_dict[el]["Journal_count"] += 1
                    disciplines_dict[el]["Publication_count"] += expected_pub
                else:
                    disciplines_dict[el] = {
                        "Journal_count": 1,
                        "Publication_count": expected_pub
                    }

            while y < expected_pub:
                sub_dict = {key: "" for key in csv}
                sub_dict["id"] = internal_id_pub+str(publication_number)+" "+publication_base+str(publication_number)
                if type(row["Print ISSN"]) == type(str()) and  type(row["Online ISSN"]) == type(str()):
                    oc_meta_coverage_dict["issn"] = "['"+row["Print ISSN"]+"', '"+row["Online ISSN"]+"']"
                    sub_dict["venue"] = "["+internal_id_issue+str(issue_number)+" issn:"+row["Print ISSN"]+" issn:"+row["Online ISSN"]+"]"
                elif type(row["Print ISSN"]) == type(str()): 
                    oc_meta_coverage_dict["issn"] = "['"+row["Print ISSN"]+"']"
                    sub_dict["venue"] = "["+internal_id_issue+str(issue_number)+" issn:"+row["Print ISSN"]+"]"
                elif type(row["Online ISSN"]) == type(str()):
                    oc_meta_coverage_dict["issn"] = "['"+row["Online ISSN"]+"']"
                    sub_dict["venue"] = "["+internal_id_issue+str(issue_number)+" issn:"+row["Online ISSN"]+"]"
                sub_dicts.append(sub_dict)
                y +=1
                publication_number+=1
                issue_number +=1
            

            oc_meta_coverage_dict["EP_id"] = row["Journal ID"]
            oc_meta_coverage_dict["Publications_in_venue"] = expected_pub
            oc_meta_coverage_dict["Open Access"] = True if row["Online ISSN"] in doaj_issn or row["Print ISSN"] in doaj_issn else "Unknown"
            oc_meta_coverage_prep.append(oc_meta_coverage_dict)

            expected_pub +=1 

        pd.DataFrame(sub_dicts).to_csv(path)
        x +=2
    # OC_Meta_coverage.csv 
    pd.DataFrame(oc_meta_coverage_prep).to_csv("OCMetaCoverage.csv")
    # countries.csv
    pd.DataFrame.from_dict(country_dict, orient="index").sort_values(by="Publication_count", ascending=False).to_csv("countriesFake.csv")
    # disciplines.csv
    pd.DataFrame.from_dict(disciplines_dict, orient="index").sort_values(by="Publication_count", ascending=False).to_csv("disciplinesFake.csv")


                



# ---------------------------- LAUNCH THE TEST ------------------------- #

create_test_sets(8, 0)
subprocess.run(['python', python_file, '--oc_meta', 'fake_OC_Meta', "--erih_plus", "erih_test.csv", "--doaj", "doaj_test.csv"])

## Check 
results = pd.read_csv("OCMeta_DOAJ_ErihPlus_merged.csv").sort_values(by='Publications_in_venue', ignore_index=True).drop(["Unnamed: 0"], axis=1)
results_test = pd.read_csv("OCMetaCoverage.csv").drop(["Unnamed: 0"], axis=1)

if results.equals(results_test):
    print("TEST1: Success ------> OCMeta_DOAJ_ErihPlus_merged.csv is correct")

result_countries = pd.read_csv("countries.csv")
result_test_countries = pd.read_csv("countriesFake.csv").rename(columns={'Unnamed: 0': 'Country'})

if result_countries.equals(result_test_countries):
    print("TEST2: Success ------> countries.csv is correct")
else:
    """ for idx, row in result_countries:
        for el in ["Country"]:
            if result_countries.at[idx, el] == result_countries.at[] """

result_disciplines = pd.read_csv("disciplines.csv").sort_values("Discipline", ignore_index=True)
result_test_disciplines = pd.read_csv("disciplinesFake.csv").rename(columns={'Unnamed: 0': 'Discipline'}).sort_values("Discipline", ignore_index=True)

if result_disciplines.equals(result_test_disciplines):
    print("TEST3: Success ------> disciplines.csv is correct")


