from utils import *
import glob
import os
from tqdm import tqdm
import concurrent.futures
import functools
from process import process_meta_csv, process_file_wrapper


# ============  PROCESSOR CLASSES ============= #

class Processor(object): 
    def __init__(self, batch_size, max_workers):
            self.batch_size = batch_size
            self.max_workers = max_workers  

    def set_batch_size(self, new_batch_size): 
        self.batch_size = new_batch_size
        return True
    
    def get_batch_size(self):
        return self.batch_size
    
    def set_max_workers(self, new_max_workers):
        self.max_workers = new_max_workers
        return True
    
    def get_max_workers(self):
        return self.max_workers
    


class PlayaristsProcessor(Processor): 
    
    def __init__(self, batch_size, max_workers, meta_path : str, erih_path : str, doaj_path : str):  
        self.meta_path = glob.glob(os.path.join(meta_path, "*.csv"))      
        self.erih_df = load_data(erih_path)
        self.doaj_df = load_data(doaj_path)
        super().__init__(batch_size, max_workers)
        

    def get_erih_df(self):
        return self.erih_df
    
    def get_doaj_df(self):
        return self.doaj_df

    def process_files(self):
        erih_dict = get_erih_plus_dict(self.erih_df) 
        all_results = []
        totalOCMpublications = 0
        with tqdm(total=len(self.meta_path), desc="Batches") as pbar:
            for i in range(0, len(self.meta_path), self.batch_size):
                batch_files = self.meta_path[i:i+self.batch_size]
                with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                    results = executor.map(process_file_wrapper, [(f, erih_dict) for f in batch_files])
                    all_results.extend(results)
                    
                pbar.update(len(batch_files))

        results_dict = {}
        for filename, result, publications_count in all_results:
            results_dict[filename] = result
            totalOCMpublications += publications_count
            
        #this is meta-erih merged
        final_df = pd.concat(list(results_dict.values()), ignore_index=True)
        final_df = final_df.groupby(['OC_omid', 'issn', 'EP_id']).agg({'Publications_in_venue': 'sum'}).reset_index()

        #this is meta-erih-doaj merged
        new_final_df = process_doaj_file(self.doaj_df, final_df)
        new_final_df.to_csv("SSH_Publications_in_OC_Meta_and_Open_Access_status.csv")
        
        publications_coverage_count = new_final_df.shape[0]
        OCMeta_coverage = publications_coverage_count / totalOCMpublications
        OCMeta_coverage_percent = OCMeta_coverage * 100
        print("##### OpenCitations Meta publications in ERIH PLUS: count = ", str(totalOCMpublications), "; ratio = ", str(OCMeta_coverage), "; % = ", str(OCMeta_coverage_percent))

        return new_final_df   



             

