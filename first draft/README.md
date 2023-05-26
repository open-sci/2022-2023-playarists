1- change the input directory path to your folder's path which contains csv dump files. Please pay attention that the program tested with around 30 csv dump files.

2- if you do not have multi core processor CPU, it would be better to set max_workes = 1, in main.py:
with concurrent.futures.ProcessPoolExecutor(max_workers=1) as executor
			
3- be sure you have libraries installed in your python virtual env.

run main.py