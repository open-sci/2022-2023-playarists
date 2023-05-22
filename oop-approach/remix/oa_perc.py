import pandas as pd
from utils1 import getPercentage

oa_perc = pd.read_csv("oop-approach/remix/meta_coverage.csv")

# Assuming you have a dataframe called 'df' and a column named 'column_name'
# Replace 'df' with the actual name of your dataframe and 'column_name' with the actual name of the column

value_counts = oa_perc['Open Access'].value_counts()

# 'value_counts' is now a pandas Series containing the counts of each unique value in the column

# If you want to count the occurrences of a specific value, you can access it by indexing the Series using the value
true_count = 'True'
true = value_counts[true_count] #3299
unknown_count = 'Uknown'
unknown = value_counts[unknown_count] #5390
total = len(oa_perc['Open Access']) #8691

print(getPercentage(true, total)) # 37.96% are Open Access