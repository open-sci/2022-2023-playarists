import pandas as pd
from utils import detect_delimiter

delimiter = detect_delimiter('Workflow-Steps-1.1-1.2\ERIHPLUSapprovedJournals.csv')
erih_plus_df = pd.read_csv('Workflow-Steps-1.1-1.2\ERIHPLUSapprovedJournals.csv', sep=delimiter)

erih_dict ={}
'def erih_plus_dict(erih_dict, e_df):'
for idx, row in erih_plus_df.iterrows():    
    issn1 =str(row['Print ISSN'])
    issn2 = str(row['Online ISSN'])
if issn1.notna() and issn2.notna():
    key = issn1 + issn2    
if 


    erih_dict[key] = {'country': row['Country of Publication'], 'discipline': row['ERIH PLUS Disciplines']}
    print(erih_dict)
    

 




' return erih_dict'











'erih_dict = erih_plus_dict(dict(), erih_plus_dict )'