from sklearn.metrics import r2_score
import pandas as pd

# Load ground truth and computed monthly averages
ground_truth = pd.read_csv("monthly_ground_truth.csv")
computed_monthly_averages = pd.read_csv("computed_monthly_averages.csv")

# Merge data on the common index
merged_data = pd.merge(ground_truth, computed_monthly_averages, left_index=True, right_index=True)

# Extract columns for comparison
true_values = merged_data.iloc[:, :len(ground_truth.columns)]
predicted_values = merged_data.iloc[:, len(ground_truth.columns):]

# Compute R2 score
r2 = r2_score(true_values, predicted_values)

# Check consistency
if r2 >= 0.9:
    print("Consistent")
else:
    print("Not Consistent")