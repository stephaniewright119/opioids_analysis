"""
This script processes a subset of prescription data stored in a Parquet file, calculates yearly averages of 
specific metrics, and saves the results to a new Parquet file with renamed columns.

Steps:
1. Validates the existence of the input Parquet file.
2. Reads the input Parquet file into a Dask DataFrame.
3. Converts the `TRANSACTION_DATE` column to a datetime format.
4. Extracts the year from the `TRANSACTION_DATE` column and creates a new column `TRANSACTION_YEAR`.
5. Groups the data by `BUYER_STATE`, `BUYER_COUNTY`, and `TRANSACTION_YEAR` to calculate the average values of:
   - `CALC_BASE_WT_IN_GM` renamed to `AVG_CALC_BASE_WT_IN_GM`.
   - `MME` renamed to `AVG_MME`.
6. Resets the index to ensure the grouped data is in DataFrame format.
7. Computes the Dask DataFrame and saves the resulting data to a new Parquet file.

"""

import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import os

# File paths
input_parquet_path = "./20_intermediate_files/prescription_subset.parquet"
output_parquet_path = "./20_intermediate_files/prescription_avg_gross.parquet"

# Check if the Parquet file exists
if not os.path.exists(input_parquet_path):
    raise FileNotFoundError(f"File not found: {input_parquet_path}")

# Read the Parquet file as a Dask DataFrame
print("Reading the Parquet file...")
prescription_data = dd.read_parquet(input_parquet_path)

# Convert the TRANSACTION_DATE column to datetime
print("Processing TRANSACTION_DATE column...")
prescription_data["TRANSACTION_DATE"] = dd.to_datetime(
    prescription_data["TRANSACTION_DATE"], errors="coerce"
)

# Extract the year from the TRANSACTION_DATE column
prescription_data["TRANSACTION_YEAR"] = prescription_data["TRANSACTION_DATE"].dt.year

# Compute gross opioids and MME shipped in each state for each year
print("Calculating gross opioids and MME shipped per state per year...")
state_gross = (
    prescription_data.groupby(["BUYER_STATE", "TRANSACTION_YEAR"])[
        "CALC_BASE_WT_IN_GM", "MME"
    ]
    .sum()
    .reset_index()
)

# Rename columns for clarity
state_gross = state_gross.rename(
    columns={"CALC_BASE_WT_IN_GM": "STATE_GROSS_BASE_WT", "MME": "STATE_GROSS_MME"}
)

# Group by BUYER_STATE, BUYER_COUNTY, and TRANSACTION_YEAR, and calculate averages
print("Calculating averages...")
prescription_grouped_avg = prescription_data.groupby(
    ["BUYER_STATE", "BUYER_COUNTY", "TRANSACTION_YEAR"]
)["CALC_BASE_WT_IN_GM", "MME"].mean()

# Reset the index to make the result a DataFrame
prescription_grouped_avg = prescription_grouped_avg.reset_index()

# Rename columns for averages
prescription_grouped_avg = prescription_grouped_avg.rename(
    columns={"CALC_BASE_WT_IN_GM": "AVG_CALC_BASE_WT_IN_GM", "MME": "AVG_MME"}
)

# Merge the state-level gross opioids and MME data
print("Merging gross opioids and MME data...")
prescription_grouped_avg = prescription_grouped_avg.merge(
    state_gross, on=["BUYER_STATE", "TRANSACTION_YEAR"], how="left"
)

# Compute the result and save it as a new Parquet file
print("Saving results to Parquet file...")
with ProgressBar():
    result_df = prescription_grouped_avg.compute()
    result_df.to_parquet(output_parquet_path)

print(f"Averages and gross data saved to Parquet file at {output_parquet_path}")
