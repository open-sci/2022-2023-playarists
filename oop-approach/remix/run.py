from OCMeta_coverage_classes import *

def main():

    erih_path = ERIHPlusProcessor("oop-approach\ERIHPLUSapprovedJournals.csv")

    #(erih_path.get_erih_path())
    #print(erih_path.set_erih_path("Workflow-Steps-1.1-1.2\ERIHPLUSapprovedJournals.csv"))
    #print(erih_path.get_erih_path())

    erih_df = erih_path.load_erih_df()
    erih_dict = erih_path.get_erih_plus_dict()

    #print(erih_df) #11128 rows
    #print(erih_dict)
    #print(len(erih_dict)) # 18345 issn

    # ==================================================

    doaj_path = DOAJProcessor("oop-approach\journalcsv__doaj.csv")
    doaj_df = doaj_path.load_doaj_df()

    #print(doaj_path.get_doaj_path())
    #print(doaj_path.set_doaj_path("Workflow-Steps-1.1-1.2\journalcsv__doaj.csv"))
    #print(doaj_path.get_doaj_path())

    # ====================================================

    processor = OCMetaProcessor(150, 4, "F:\DATAvarie\csv\csv_dump")

    #print(processor.set_batch_size(150))
    #print(processor.get_batch_size())
    #print(processor.set_max_workers(4))
    #print(processor.get_max_workers())

    meta_coverage = processor.process_files(erih_dict, doaj_df)
    meta_coverage.to_csv('meta_coverage.csv', index=False)


if __name__ == '__main__':
    main()