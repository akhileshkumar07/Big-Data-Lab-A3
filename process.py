import pandas as pd
import glob

FILEPATH = '/home/akhilesh/big-data-lab/'

# Construct the pattern for CSV files
pattern = FILEPATH + '*.csv'
# Use glob to match files
csv_files = glob.glob(pattern)

FILENAME = csv_files[0]
df = pd.read_csv(FILENAME)

# Compute monthly averages from day-wise data points


# Save computed monthly averages to a new CSV file
monthly_averages.to_csv("computed_monthly_averages.csv")