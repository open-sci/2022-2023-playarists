from OCMeta_coverage_classes import *
from Disciplines_Countries_classes import *
from utils1 import load_data


def main():

    # TO RUN ALL WORKFLOW

    # args are: batch_size, max_workers, OCmeta_path, erih_path, doaj_path
    meta_coverage = PlayaristsProcessor(150, 4, "F:\DATAvarie\csv\csv_dump", "oop-approach\ERIHPLUSapprovedJournals.csv", "oop-approach\journalcsv__doaj.csv")

    countries = CountriesProcessor(meta_coverage)
    countries_dict = countries.create_countries_dict() #this is a tuple
    countries_dict, no_country_venues = countries_dict
    
    print(no_country_venues) # this is for us to know which venues have no countries information

    disciplines = DisciplinesProcessor(meta_coverage)
    disciplines_dict = disciplines.create_disciplines_dict()

    result_countries = CountsProcessor(meta_coverage, "oop-approach/remix/countries.csv")
    result_countries = result_countries.counts(countries_dict, "Country")

    result_disciplines = CountsProcessor(meta_coverage, "oop-approach/remix/disciplines.csv")
    result_disciplines = result_disciplines.counts(disciplines_dict, "Discipline")
    # N.B. we can slightly modify the code to process these objects automatically by adding a method "def add_disciplinesCountriesProcessor" 
    # that appends the object to a list over which the counts method iterates ??

if __name__ == '__main__':
    main()