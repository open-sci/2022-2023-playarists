import argparse
import os
import pandas as pd
import glob
import concurrent.futures
from tqdm import tqdm
from utils import detect_delimiter
from process import process_file, process_file_wrapper
from doaj_processing import process_doaj_file


class CSV_to_DataFrame(object):
    def __init__(self, path):
        self.path = path
        self.delimiter = detect_delimiter(path)
        self.dataframe = pd.read_csv(path, delimiter=self.delimiter)
        #self.erih_dict = self.get_erih_plus_dict() #makes no sense to have this as an attribute when it is defined as a method

    def get_erih_plus_dict(self):
        df = self.dataframe
        erih_plus_dict = {}
        for idx, row in df.iterrows():
            erih_plus_dict[row["Print ISSN"]] = row["Journal ID"]
            erih_plus_dict[row["Online ISSN"]] = row["Journal ID"]
        return erih_plus_dict
    
erih_plus = CSV_to_DataFrame("Workflow-Steps-1.1-1.2\ERIHPLUSapprovedJournals.csv")

print(erih_plus.get_erih_plus_dict())

def main():
    # Parameters
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_size", default=150, type=int, required=False, help="batch size: e.g. 100")
    parser.add_argument("--max_workers", default=8, type=int, required=False, help="max_workers: e.g. 4")
    parser.add_argument("--erih_plus", default="ERIHPLUSapprovedJournals.csv", type=str, required=False, help="path to the ERIH PLUS dataset")
    args = parser.parse_args()
    
    input_directory = "dumps"
    files = glob.glob(os.path.join(input_directory, "*.csv"))
    all_results = []

    erih_dict = CSV_to_DataFrame(args.erih_plus).get_erih_plus_dict()

    # Initialize a progress bar to visualize the progress of processing batches of files
    with tqdm(total=len(files), desc="Batches") as pbar:
        # Process files in batches
        for i in range(0, len(files), args.batch_size):
            # Get the current batch of files
            batch_files = files[i:i + args.batch_size]

            # Process the current batch of files using a ProcessPoolExecutor for parallelism
            with concurrent.futures.ProcessPoolExecutor(max_workers=args.max_workers) as executor:
                results = executor.map(process_file_wrapper, [(f, erih_dict) for f in batch_files])
                all_results.extend(results)
            # Update the progress bar for each batch
            pbar.update(len(batch_files))

    # Create a dictionary to store the results
    results_dict = {filename: result for filename, result in all_results}
    print("expected output for part 1.1:")
    # Combine the results from all batches into a single DataFrame
    final_df = pd.concat(list(results_dict.values()), ignore_index=True)
    final_df = final_df.groupby(['OC_omid', 'issn', 'EP_id']).agg({'Publications_in_venue': 'sum'}).reset_index()
    print(final_df)

    # Process the DOAJ file and merge the Open Access information
    #final_df = process_doaj_file(final_df)
    print("expected output for part 1.2:")
    print(final_df)
    final_df.to_csv('resultDf.csv', index=False)

if __name__ == '__main__':
    main()

