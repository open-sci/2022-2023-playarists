import pandas as pd
from pprint import pprint


def retrieve_doaj_country(unmatched_issn, result_df, doaj_df, countr_dict):

    # subdataframe with only unmatched countries rows
    no_country_df =  result_df[result_df["EP_id"].isin(unmatched_issn)]
    no_country_df.reset_index(drop=True, inplace=True)

    # this is to fix iss col data so that it's actually a list 
    for idx, row in no_country_df["issn"].items():
            a_list = []
            for el in row[1:-1].split(", "):
                el = el.replace("'", "")
                a_list.append(el)
            row = a_list
            no_country_df.at[idx, "issn"] = row 
              
    no_country_df  = no_country_df.explode('issn') # explodes issn lists
    
    new_country = pd.merge(no_country_df, doaj_df, left_on='issn', right_on='Journal ISSN (print version)', how='left') #if I do 'inner' than I cannot match the print issn
    new_country = pd.merge(new_country, doaj_df, left_on='issn', right_on='Journal EISSN (online version)',how='left')
    
    # drop useless columns
    new_country = new_country[['EP_id', 'Country of publisher_x', 'Country of publisher_y' ]]
    # merge countries values coming from print and online issn
    new_country['Country'] = new_country['Country of publisher_x'].fillna(new_country['Country of publisher_y'])
    # keep only new complete Country column and drop all venues without country
    new_country = new_country.drop(['Country of publisher_x', 'Country of publisher_y'], axis=1)
    # drop duplicates
    new_country.drop_duplicates(inplace=True) 

    #retrieve venues id of unmatched venue-country
    mask_na_countries =  pd.isna(new_country['Country'])
    unmatched_df = new_country[mask_na_countries]
    unmatched_df = unmatched_df[['EP_id']]
    #print(unmatched_df)

    new_country = new_country.dropna(subset=['Country']).reset_index(drop=True)

    #extend country dict
    for idx, row in new_country.iterrows():
        if row['Country'] not in countr_dict:          
            countr_dict[row['Country']] = [row['EP_id']]

        else:
             countr_dict[row['Country']].append(row['EP_id'])
    
    # fix double countries
    if 'Turkey' in countr_dict and 'Türkiye' in countr_dict:
        countr_dict['Turkey'].extend(countr_dict['Türkiye'])
        del countr_dict['Türkiye']
    if 'Venezuela' in countr_dict and 'Venezuela, Bolivarian Republic of' in countr_dict:
        countr_dict['Venezuela'].extend(countr_dict["Venezuela, Bolivarian Republic of"])
        del countr_dict["Venezuela, Bolivarian Republic of"]
    if 'Republic of' in countr_dict:
        del countr_dict['Republic of'] 
                 

    return countr_dict, unmatched_df


