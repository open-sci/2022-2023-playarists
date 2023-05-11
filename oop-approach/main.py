import argparse
import glob
import os
import pandas as pd
from tqdm import tqdm
from csv_to_dataframe import CSV_to_DataFrame
from process import ProcessMetaCSV
from doaj_processing import DOAJProcessor
import concurrent.futures


class PlayaristProcessor:
    def __init__(self, batch_size, max_workers, erih_plus_path, doaj_file_path):
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.erih_plus_path = erih_plus_path
        self.doaj_file_path = doaj_file_path

    def process_files(self, files):
        erih_dict = self.get_erih_plus_dict(self.erih_plus_path)
        process_meta_csv = ProcessMetaCSV(erih_dict)
        all_results = []
        with tqdm(total=len(files), desc="Batches") as pbar:
            for i in range(0, len(files), self.batch_size):
                batch_files = files[i:i+self.batch_size]
                with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                    results = executor.map(process_meta_csv.process_file_wrapper, batch_files)
                    all_results.extend(results)
                pbar.update(len(batch_files))
        
        results_dict = {filename: result for filename, result in all_results}
        final_df = pd.concat(list(results_dict.values()), ignore_index=True)
        final_df = final_df.groupby(['OC_omid', 'issn', 'EP_id']).agg({'Publications_in_venue': 'sum'}).reset_index()
        
        return final_df

    def get_erih_plus_dict(self, path):
        csv_to_dataframe = CSV_to_DataFrame(path)
        df = csv_to_dataframe.load_data()
        erih_plus_dict = {}
        for idx, row in df.iterrows():
            erih_plus_dict[row["Print ISSN"]] = row["Journal ID"]
            erih_plus_dict[row["Online ISSN"]] = row["Journal ID"]
        return erih_plus_dict

    def run_workflow(self):
        input_directory = "D:\open-sci\dump-files\opencitations-meta\partial_dump"
        files = glob.glob(os.path.join(input_directory, "*.csv"))

        final_df = self.process_files(files)

        doaj_processor = DOAJProcessor(self.doaj_file_path)

        final_df = doaj_processor.process_doaj_file(final_df)

        print("Expected output for part 1.1:")
        print(final_df)
        print("Expected output for part 1.2:")
        print(final_df)

        final_df.to_csv('resultDf.csv', index=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_size", default=150, type=int, required=False, help="batch size: e.g. 100")
    parser.add_argument("--max_workers", default=8, type=int, required=False, help="max_workers: e.g. 4")
    parser.add_argument("--erih_plus", default="ERIHPLUSapprovedJournals.csv", type=str, required=False, help="path to the ERIH PLUS dataset")
    parser.add_argument("--doaj_file", default="journalcsv__doaj.csv", type=str, required=False, help="path to the DOAJ file")
    args = parser.parse_args()

    playarist_processor = PlayaristProcessor(args.batch_size, args.max_workers, args.erih_plus, args.doaj_file)

    playarist_processor.run_workflow()


if __name__ == '__main__':
    main()
