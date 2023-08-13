from OCMeta_coverage_classes import *
from Disciplines_Countries_classes import *
from utils import load_data
import argparse


def main(args):

    # TO RUN ALL WORKFLOW

    # args are: batch_size, max_workers, OCmeta_path, erih_path, doaj_path
    meta_coverage = PlayaristsProcessor(args.batch_size, args.max_workers, args.oc_meta, args.erih_plus, args.doaj)
    
    print("##### Starting OC Meta processing:")
    meta_coverage.process_files()

    print("##### Extracting Countries:")
    countries = CountriesProcessor(meta_coverage, args.remove_megajournals)
    countries_dict = countries.create_countries_dict() 
    countries_dict, no_country_venues = countries_dict
    result_countries = CountsProcessor(meta_coverage, "SSH_Publications_and_Journals_by_Country.csv", args.remove_megajournals,)
    result_countries = result_countries.counts(countries_dict, "Country")
    # save dictionary to json
    save_countries_dict = open("result_countries.json", "w")  
    json.dump(countries_dict, save_countries_dict, indent = 6)  
    save_countries_dict.close()  
    print("##### These venues have no country specified", no_country_venues) 

    print("##### Extracting Disciplines:")
    disciplines = DisciplinesProcessor(meta_coverage, args.remove_megajournals)
    disciplines_dict = disciplines.create_disciplines_dict()
    result_disciplines = CountsProcessor(meta_coverage, "SSH_Publications_by_Discipline.csv", args.remove_megajournals)
    result_disciplines = result_disciplines.counts(disciplines_dict, "Discipline")
    # save dictionary to json
    save_disciplines_dict = open("result_disciplines.json", "w")  
    json.dump(disciplines_dict, save_disciplines_dict, indent = 6)  
    save_disciplines_dict.close()  
    
    print("##### Comparing EU and US:")
    us_eu = Compare_US_EU(meta_coverage, args.remove_megajournals)
    meta_coverage_us_eu= us_eu.compare_us_eu("ERIHPLUSapprovedJournals.csv", countries_dict) #returns a tuple with the two datasets
    meta_coverage_us, meta_coverage_eu, us_data, eu_data = meta_coverage_us_eu
    us_eu.counts_us_eu(disciplines_dict, "Discipline", meta_coverage_us, "compareUS_EU/us_disciplines_count.csv")
    us_eu.counts_us_eu(disciplines_dict, "Discipline", meta_coverage_eu, "compareUS_EU/eu_disciplines_count.csv")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("--batch_size", default=150, type=int, required=False, help="batch size: e.g. 100")
    parser.add_argument("--max_workers", default=4, type=int, required=False, help="max_workers: e.g. 4")
    parser.add_argument("--oc_meta", default="F:\DATAvarie\csv\csv_dump", type=str, required=False, help="path to the OpenCitations Meta dataset")
    parser.add_argument("--erih_plus", default="ds\ERIHPLUSapprovedJournals.csv", type=str, required=False, help="path to the ERIH PLUS dataset")
    parser.add_argument("--doaj", default="ds\journalcsv__doaj_20230528_0035_utf8.csv", type=str, required=False, help="path to the DOAJ file")
    parser.add_argument("--remove_megajournals", default=True, type=bool, required=False, help="exclude mega journals from analysis")

    args = parser.parse_args()
    main(args)