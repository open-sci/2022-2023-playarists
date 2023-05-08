import os
import pandas as pd
import glob
import concurrent.futures
from tqdm import tqdm
from utils import detect_delimiter
from process import process_file, process_file_wrapper
from doaj_processing import process_doaj_file

def main():
    delimiter = detect_delimiter('ERIHPLUSapprovedJournals.csv')
    erih_plus_df = pd.read_csv('ERIHPLUSapprovedJournals.csv', sep=delimiter)

    input_directory = "csv_dump"
    files = glob.glob(os.path.join(input_directory, "*.csv"))

    # Number of files to process at once
    batch_size = 150

    all_results = []

    # Initialize a progress bar to visualize the progress of processing batches of files
    with tqdm(total=len(files), desc="Batches") as pbar:
        # Process files in batches
        for i in range(0, len(files), batch_size):
            # Get the current batch of files
            batch_files = files[i:i + batch_size]

            # Process the current batch of files using a ProcessPoolExecutor for parallelism
            with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
                results = executor.map(process_file_wrapper, [(f, erih_plus_df) for f in batch_files])
                all_results.extend(results)
            # Update the progress bar for each batch
            pbar.update(len(batch_files))

    # Create a dictionary to store the results
    results_dict = {filename: result for filename, result in all_results}
    print("expected output for part 1.1:")
    # Combine the results from all batches into a single DataFrame
    final_df = pd.concat(list(results_dict.values()), ignore_index=True)
    print(final_df)

    # Process the DOAJ file and merge the Open Access information
    final_df = process_doaj_file(final_df)
    print("expected output for part 1.2:")
    print(final_df)
    final_df.to_csv('resultDf.csv', index=False)

if __name__ == '__main__':
    main()

