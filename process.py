import pandas as pd
import yaml

# Read FILENAME from YAML file
with open("tmp.yaml", "r") as yaml_file:
    config = yaml.safe_load(yaml_file)

FILENAME = config["FILENAME"]
print(f"Reading File: {FILENAME}")
data = pd.read_csv(FILENAME)

hourly_data = data.filter(like='Hourly', axis = 1) # Extract hourly columns
date_time = data['DATE'] # Extract the DATE column

hourly_data = pd.concat([date_time, hourly_data], axis = 1) # Merge hourly columns and DATE
hourly_data.fillna(0, axis=1, inplace=True)
hourly_data.dropna(axis=0, inplace = True)

hourly_data['DATE'] = pd.to_datetime(hourly_data['DATE'])  # Convert 'DATE' column to datetime
hourly_data['Date'] = hourly_data['DATE'].dt.date
hourly_data.drop('DATE', axis = 1, inplace = True)
hourly_data.set_index('Date', inplace=True)

#print(hourly_data.columns.values)
#print(hourly_data.shape)
#print(hourly_data.isna().sum())

COLUMNS = hourly_data.columns.values
for col in COLUMNS:
    hourly_data[col] = pd.to_numeric(hourly_data[col], errors='coerce')

# Calculate daily averages
daily_average = hourly_data.groupby('Date')[COLUMNS].mean()

print(daily_average)

daily_average.index = pd.to_datetime(daily_average.index)

daily_average['Month'] = daily_average.index.month

# Compute monthly averages
monthly_average = daily_average.groupby('Month').mean()

print(monthly_average)

# Save extracted data to a new CSV file
monthly_average.to_csv("computed_monthly_averages.csv", index=False)