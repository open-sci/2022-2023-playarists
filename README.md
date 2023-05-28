# 2022-2023-Playarist Software
 The repository for the team Playarists of the Open Science course a.a. 2022/2023

## Source data

* <a href="https://kanalregister.hkdir.no/publiseringskanaler/erihplus/periodical/listApproved">OpenCitations Meta dump</a> (downloaded 24/02/2023) 
* <a href="https://kanalregister.hkdir.no/publiseringskanaler/erihplus/periodical/listApproved">ERIH PLUS approved Journals</a> (downloaded 27/04/2023)
* <a href="https://doaj.org/docs/public-data-dump/">DOAJ public dump</a> (downloaded 06/05/2923)
 
 
 
## how to run the software
 To reuse our program, please install the requirements.txt:
```sh
$ pip install -r requirements.txt
```
 Users can run the program by cloning the repository and accessing it from shell. The command to launch the program is:
```sh
$ python run_workflow.py --batch_size number_batch_size --max_workers number__workers --oc_meta path_to_OC_Meta_folder --erih_plus path_to_erih_plus.csv --doaj path_to_doaj.csv
```
 All the parameters are already set to default values, however, users are strongly suggested to modify them depending on their system specifications (e.g.: --batch_size 100 --max_workers 4) and the names/locations of the downloaded datasets (--oc_meta, --erih_plus, --doaj) 

 
 
 
 
 
 
 
 
 
 
 
 
 
 
 ## Naming convention of Datasets 
 
| OC_omid | issn | EP_id | Publications_in_venue | Open_Access |
|---------|------|-------|-----------------------|-------------|

| Discipline | Journal_count | Publication_count | 
|------------|---------------|-------------------|

| Discipline | Journal_count | Publication_count | 
|------------|---------------|-------------------|


- run_workflow.py
- utils.py
- OCMeta_coverage_classes.py
- Disciplines_Countries_classes.py
- process
- retrieve_doaj_country.py
