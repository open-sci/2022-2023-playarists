[![DOI](https://zenodo.org/badge/627426136.svg)](https://zenodo.org/badge/latestdoi/627426136)

# 2022-2023-Playarist Software
 The repository for the team Playarists of the Open Science course a.a. 2022/2023

## Source data

* <a href="https://figshare.com/articles/dataset/OpenCitations_Meta_CSV_dataset_of_all_bibliographic_metadata/21747461/3">OpenCitations Meta dump version 3</a> (downloaded 24/02/2023) 
* <a href="https://kanalregister.hkdir.no/publiseringskanaler/erihplus/periodical/listApproved">ERIH PLUS approved Journals</a> (downloaded 27/04/2023)
* <a href="https://doaj.org/docs/public-data-dump/">DOAJ public dump</a> (downloaded 06/05/2923)
 
 
 
## How to run the software
 To reuse our program, please install the requirements.txt:
```sh
pip install -r requirements.txt
```
 Users can run the program by cloning the repository and accessing it from shell. The command to launch the program is:
```sh
python run_workflow.py --batch_size number_batch_size --max_workers number__workers --oc_meta path_to_OC_Meta_folder --erih_plus path_to_erih_plus.csv --doaj path_to_doaj.csv
```
 All the parameters are already set to default values, however, users are strongly suggested to modify them depending on their system specifications (e.g.: --batch_size 100 --max_workers 4) and the names/locations of the downloaded datasets (--oc_meta, --erih_plus, --doaj). In case users do not need to specify any parameter, the command is as follows:
 ```sh
python run_workflow.py
``` 
 
 
 ## Naming convention of Datasets 

For the sake of clarity, we have used some predefined labels to identify the main entities of our research: they will be introduced as they are split in each of the resulting CSV files.

`SSH_Publications_in_OC_Meta_and_Open_Access_status.csv`:
| OC_omid | issn | EP_id | Publications_in_venue | Open_Access |
|---------|------|-------|-----------------------|-------------|

Each publication venue is associated with its identifier [ISSN]. Items included in OpenCitations Meta and/or Erih-PLUS are also identified by their internal IDs [OC_omid] and/or [EP_id]. [Publications_in_venue] refers to the numbers of publications in each venue, while [Open_access] expresses whether the venue's Open Access status is True or Unknown.

`SSH_Publications_by_Discipline.csv`:
| Discipline | Journal_count | Publication_count | 
|------------|---------------|-------------------|

Each discipline is associated with a label [Discipline] and the number of journals/publications referring to it: [Journal_count] and [Publication_count].

`SSH_Publications_by_Discipline.csv`:
| Country | Journal_count | Publication_count | 
|------------|---------------|-------------------|

Each country of publication is associated with its name [Country] and the number of journals/publications published ther: [Journal_count] and [Publication_count].

Besides, the resulting csv will also be produced by running the software:

- <b>meta_coverage_us</b>: The dataset SSH_Publications_in_OC_Meta_and_Open_Access_status.csv filtered to contain only US Journals
- <b>meta_coverage_eu</b>: The dataset SSH_Publications_in_OC_Meta_and_Open_Access_status.csv filtered to contain only US Journals

- <b>us_data</b>: For US Journals covered in OCMeta, contains<br>

EP_id | Publications_in_venue | Original Title | Country of Publication | ERIH PLUS Disciplines | disc_count
---------|---------------------|--------------|----------------------|---------------------|------------

the correlation between Publications_in_venue and the number of disciplines is visualized in the scatterplot "scatter_correlation_ndisc_npub_US"

- <b>eu_data</b>:  For UK Journals covered in OCMeta, contains<br>

EP_id | Publications_in_venue | Original Title | Country of Publication | ERIH PLUS Disciplines | disc_count
---------|---------------------|--------------|----------------------|---------------------|------------

the correlation between Publications_in_venue and the number of disciplines is visualized in the scatterplot "scatter_correlation_ndisc_npub_UK"

- <b>us_disciplines_count</b>: contains the count of US Journals and theit Publications for each Discipline

Discipline | Journal_count | Publication_count
---------|---------------------|--------------

- <b>eu_disciplines_count</b>: contains the count of UK Journals and theit Publications for each Discipline

Discipline | Journal_count | Publication_count
---------|---------------------|--------------

## Extra
* <a href="https://ghasempouri1984-2022-2023-playarists-code-streamlit-app-1aspl5.streamlit.app/">streamlit visualization of results</a>

