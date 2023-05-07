1- change the input directory path, to your folder path which contains csv dump files.
	* Now test it with just 22 files. not the whole csv dump please.

2- if you do not have multi core processor CPU, it would be better to set max_workes = 1, in this line (number 29) main.py:
			with concurrent.futures.ProcessPoolExecutor(max_workers=1) as executor
			
3- be sure you have libraries installed in your python virtual env.

run main.py