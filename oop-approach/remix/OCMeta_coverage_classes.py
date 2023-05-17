from utils1 import *
import glob
import os
from tqdm import tqdm
import concurrent.futures
import functools
from process1 import process_meta_csv, process_file_wrapper


# ================= ERIH-PLUS CLASSES ================= #
    
class ERIHPlusProcessor(object):
    def __init__(self, erih_path : str): #erih_df is a ERIHPlusDataProcessor object
        self.erih_path = erih_path

    def set_erih_path(self, new_path): #method to change path
        self.erih_path = new_path
        return True

    def get_erih_path(self): #method to get path
        return self.erih_path
    
    
    def load_erih_df(self):
        return load_data(self.erih_path)

    def get_erih_plus_dict(self): #method to load dictionary

        erih_df = load_data(self.erih_path) 
        erih_plus_dict = {}
        for idx, row in erih_df.iterrows():
            erih_plus_dict[row["Print ISSN"]] = row["Journal ID"]
            erih_plus_dict[row["Online ISSN"]] = row["Journal ID"]
        return erih_plus_dict
    
    
# =================== DOAJ CLASSES ================= #

class DOAJProcessor(object):
    def __init__(self, doaj_path :str):
        self.doaj_path = doaj_path
    
    def set_doaj_path(self, new_path): #method to change path
        self.doaj_path = new_path
        return True

    def get_doaj_path(self): #method to get path
        return self.doaj_path
    
    def load_doaj_df(self):
        return load_data(self.doaj_path)

    def process_doaj_file(self, merged_data):  #this is called in ocmetaprocessor

        # Load DOAJ CSV file into a DataFrame
        doaj_df = load_data(self.doaj_path)
        new_doaj = doaj_df.iloc[1:, [5, 6, 10]]

        # Create a dictionary of Open Access ISSNs
        open_access_dict = {}
        for index, row in new_doaj.iterrows():
            open_access_dict[row['Journal ISSN (print version)']] = True
            open_access_dict[row['Journal EISSN (online version)']] = True
        
        open_access_keys = list(open_access_dict.keys())

        merged_data['Open Access'] = "Uknown"
        for idx, row in merged_data["issn"].items():
            for el in row[1:-1].split(", "):
                el = el.replace("'", "")
                if el in open_access_keys:
                    merged_data.at[idx, 'Open Access'] = True

        return merged_data



# ============ META PROCESSOR CLASSES ============= #

#this is the class that does everything basically 
#only, instead of creating objects on the fly, I am trying to initialize them as class attrs                         
class OCMetaProcessor(object): 
    
    def __init__(self, batch_size, max_workers, meta_path : str):  
        self.meta_path = glob.glob(os.path.join(meta_path, "*.csv"))      
        self.batch_size = batch_size
        self.max_workers = max_workers             

    def set_meta_path(self, new_path): #method to change path
        self.meta_path = glob.glob(os.path.join(new_path, "*.csv"))
        return True

    def get_meta_path(self): #method to get path
        return self.meta_path

    def set_batch_size(self, new_batch_size): #should we specify dtype?
        self.batch_size = new_batch_size
        return True
    
    def get_batch_size(self):
        return self.batch_size
    
    def set_max_workers(self, new_max_workers):
        self.max_workers = new_max_workers
        return True
    
    def get_max_workers(self):
        return self.max_workers


    def process_files(self, erih_dict, doaj_df):
        all_results = []
        with tqdm(total=len(self.meta_path), desc="Batches") as pbar:
            for i in range(0, len(self.meta_path), self.batch_size):
                batch_files = self.meta_path[i:i+self.batch_size]
                with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                                        
                    #I went back to how it was written in the first scripts
                    results = executor.map(process_file_wrapper, [(f, erih_dict) for f in batch_files])
                    all_results.extend(results)
                    
                pbar.update(len(batch_files))

        results_dict = {filename: result for filename, result in all_results}
        #this is meta-erih merged
        final_df = pd.concat(list(results_dict.values()), ignore_index=True)
        final_df = final_df.groupby(['OC_omid', 'issn', 'EP_id']).agg({'Publications_in_venue': 'sum'}).reset_index()

        #this is meta-erih-doaj merged
        new_final_df = doaj_df.process_doaj_file(final_df)
        
        return new_final_df   
        #missing line of code where we export to csv (if we want)         
             

