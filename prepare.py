import pandas as pd
import os
import random
import yaml

FILEPATH = os.getcwd()
files = os.listdir(FILEPATH)

# Filter only CSV files
csv_files = [file for file in files if file.endswith('.csv')]
print(csv_files)

FILENAME = random.choice(csv_files)
# Write FILENAME to a YAML file
with open("tmp.yaml", "w") as yaml_file:
    yaml.dump({"FILENAME": FILENAME}, yaml_file)

df = pd.read_csv(FILENAME)

# Extract monthly aggregate columns & column names
monthly_aggregates = df.filter(like='Monthly', axis=1)

# Save extracted data to a new CSV file
monthly_aggregates.to_csv("monthly_ground_truth.csv", index=False)