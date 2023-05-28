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
    result_countries = CountsProcessor(meta_coverage, "/home/runner/work/test-workflow/test-workflow/countries.csv")
    result_countries = result_countries.counts(countries_dict, "Country")
    print("##### These venues have no country specified", no_country_venues) 

    print("##### Extracting Disciplines:")
    disciplines = DisciplinesProcessor(meta_coverage)
    disciplines_dict = disciplines.create_disciplines_dict()
    result_disciplines = CountsProcessor(meta_coverage, "/home/runner/work/test-workflow/test-workflow/disciplines.csv")
    result_disciplines = result_disciplines.counts(disciplines_dict, "Discipline")
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("--batch_size", default=150, type=int, required=False, help="batch size: e.g. 100")
    parser.add_argument("--max_workers", default=4, type=int, required=False, help="max_workers: e.g. 4")
    parser.add_argument("--oc_meta", default="/home/runner/work/test-workflow/test-workflow/csv_dump", type=str, required=False, help="path to the OpenCitations Meta dataset")
    parser.add_argument("--erih_plus", default="/home/runner/work/test-workflow/test-workflow/ERIHPLUSapprovedJournals.csv", type=str, required=False, help="path to the ERIH PLUS dataset")
    parser.add_argument("--doaj", default="/home/runner/work/test-workflow/test-workflow/journalcsv__doaj.csv", type=str, required=False, help="path to the DOAJ file")

    args = parser.parse_args()
    args.oc_meta = args.oc_meta.path if isinstance(args.oc_meta, argparse.FileType) else args.oc_meta
    args.erih_plus = args.erih_plus.path if isinstance(args.erih_plus, argparse.FileType) else args.erih_plus
    args.doaj = args.doaj.path if isinstance(args.doaj, argparse.FileType) else args.doaj
    main(args)
