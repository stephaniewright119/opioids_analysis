"""
Due to the size of the prescription dataset, the .tsv file was saved locally 
and the script run to extract the relevant columns and save it as a parquet file.
The .tsv was then deleted as it exceeds the limits of git lfs.
"""

import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import os

# Correct file path
tsv_path = "./00_data/arcos_all.tsv"

# Check if the file exists
if not os.path.exists(tsv_path):
    raise FileNotFoundError(f"File not found: {tsv_path}")

# Specify the data types of the problematic columns
dtypes = {"NDC_NO": "object", "REPORTER_ADDL_CO_INFO": "object"}

# Read the TSV file in chunks
print("Reading the TSV file in chunks...")
ddf = dd.read_csv(tsv_path, sep="\t", blocksize="500MB", dtype=dtypes)

# Select the desired columns
print("Selecting desired columns...")
ddf_subset = ddf[
    ["BUYER_STATE", "BUYER_COUNTY", "CALC_BASE_WT_IN_GM", "TRANSACTION_DATE", "MME"]
]

# Update the Parquet file path to avoid conflict
parquet_path = "./20_intermediate_files/prescription_subset.parquet"

# Write the subset to a Parquet file with progress tracking
print("Writing subset to Parquet file...")
with ProgressBar():
    ddf_subset.to_parquet(parquet_path)

print(f"Subset saved to Parquet file at {parquet_path}")
