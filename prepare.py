import pandas as pd
import glob
from tqdm.auto import tqdm
tqdm.pandas()

FILEPATH = '/home/akhilesh/big-data-lab/'

# Construct the pattern for CSV files
pattern = FILEPATH + '*.csv'
# Use glob to match files
csv_files = glob.glob(pattern)

FILENAME = csv_files[0]
df = pd.read_csv(FILENAME)

# Extract monthly aggregate columns & column names
monthly_aggregates = df.filter(like='Monthly', axis=1)
column_names = list(monthly_aggregates.columns.values)

# Save extracted data to a new CSV file
monthly_aggregates.to_csv("monthly_ground_truth.csv", index=False)

# Define the file path for writing column names
cols_file = "column_names.txt"

# Open the file in write mode
with open(cols_file, "w") as f:
    # Write each column name followed by a newline character
    for column_name in column_names:
        f.write(column_name + "\n")

print("Column names have been written to", cols_file)