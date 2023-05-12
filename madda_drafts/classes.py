class DataProcessor(object):
    def __init__(self): #initializes the obj with attrs
        self.erih_path = ""
        self.ocmeta_path = ""
        self.doaj_path = ""
    
    def get_erih_path(self): # to retrieve the path
        return self.erih_path
    
    def get_ocmeta_path(self):
        return self.ocmeta_path
    
    def get_doaj_path(self):
        return self.doaj_path
    
    def set_erih_path(self, path): # to retrieve the path
        self.erih_path = path
        return self.erih_path
    
    def get_ocmeta_path(self, path):
        self.ocmeta_path = path
        return self.ocmeta_path
    
    def get_doaj_path(self, path):
        self.doaj_path = path
        return self.doaj_path
    

class ResutsProcessor(object):
    def __init__(self):
        self.results = [] 
# I might want to use a method to call and retrieve the dataframe we need

class countriesProcessor():
    def __init__(self):


