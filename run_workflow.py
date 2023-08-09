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
    countries = CountriesProcessor(meta_coverage)
    countries_dict = countries.create_countries_dict() 
    countries_dict, no_country_venues = countries_dict
    result_countries = CountsProcessor(meta_coverage, "SSH_Publications_and_Journals_by_Country.csv")
    result_countries = result_countries.counts(countries_dict, "Country")
    print("##### These venues have no country specified", no_country_venues) 

    print("##### Extracting Disciplines:")
    disciplines = DisciplinesProcessor(meta_coverage)
    disciplines_dict = disciplines.create_disciplines_dict()
    result_disciplines = CountsProcessor(meta_coverage, "SSH_Publications_by_Discipline.csv")
    result_disciplines = result_disciplines.counts(disciplines_dict, "Discipline")
    
    print("##### Comparing UK and US:")
    us_uk = Compare_US_UK(meta_coverage)
    meta_coverage_us_uk= us_uk.compare_us_uk("ds/ERIHPLUSapprovedJournals.csv", countries_dict) #returns a tuple with the two datasets
    meta_coverage_us, meta_coverage_uk, us_data, uk_data = meta_coverage_us_uk
    disciplines_count_us = us_uk.counts_us_uk(disciplines_dict, "Discipline", meta_coverage_us, "compareUS_UK/us_disciplines_count.csv")
    disciplines_count_uk = us_uk.counts_us_uk(disciplines_dict, "Discipline", meta_coverage_uk, "compareUS_UK/uk_disciplines_count.csv")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("--batch_size", default=150, type=int, required=False, help="batch size: e.g. 100")
    parser.add_argument("--max_workers", default=4, type=int, required=False, help="max_workers: e.g. 4")
    parser.add_argument("--oc_meta", default="F:\DATAvarie\csv\csv_dump", type=str, required=False, help="path to the OpenCitations Meta dataset")
    parser.add_argument("--erih_plus", default="ds\ERIHPLUSapprovedJournals.csv", type=str, required=False, help="path to the ERIH PLUS dataset")
    parser.add_argument("--doaj", default="ds\journalcsv__doaj_20230528_0035_utf8.csv", type=str, required=False, help="path to the DOAJ file")

    args = parser.parse_args()
    main(args)