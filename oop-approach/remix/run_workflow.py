from OCMeta_coverage_classes import *
from Disciplines_Countries_classes import *
from utils1 import load_data


def main():

    erih_path = ERIHPlusProcessor("oop-approach\ERIHPLUSapprovedJournals.csv")
    erih_df = erih_path.load_erih_df()
    erih_dict = erih_path.get_erih_plus_dict()

    # ==================================================

    doaj_path = DOAJProcessor("oop-approach\journalcsv__doaj.csv")
    doaj_df = doaj_path.load_doaj_df()

    # ====================================================

    processor = OCMetaProcessor(150, 4, "F:\DATAvarie\csv\csv_dump")
    #meta_coverage = processor.process_files(erih_dict, doaj_path) #8690 rows 
    #meta_coverage.to_csv('meta_coverage.csv', index=False)

    # ======================================================
    meta_coverage = load_data("oop-approach/remix/meta_coverage.csv") #I commented out the class method above and 
                                                                    # just loaded the resultDF from workflow step one for practicality

    countries = CountriesProcessor(erih_df, meta_coverage, doaj_df)
    countries_dict = countries.create_countries_dict() #this is a tuple
    countries_dict, no_country_venues = countries_dict
    print(no_country_venues)

    disciplines = DisciplinesProcessor(erih_df, meta_coverage)
    disciplines_dict = disciplines.create_disciplines_dict()

    # ==================================================

    #this is now done manually but instead it could iterate over CountriesDisciplinesProcessor objects, by adding the method to add the object

    result_countries = ResultsProcessor(meta_coverage, "oop-approach/remix/countries.csv")
    result_countries = result_countries.counts(countries_dict, "Country")

    result_disciplines = ResultsProcessor(meta_coverage, "oop-approach/remix/disciplines.csv")
    result_disciplines = result_disciplines.counts(disciplines_dict, "Discipline")

if __name__ == '__main__':
    main()