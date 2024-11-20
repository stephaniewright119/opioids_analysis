import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import os

# File paths
tsv_path = "./00_data/arcos_all.tsv"
parquet_path = "./20_intermediate_files/prescription_subset_more_drugs.parquet"

# List of target drugs
target_drugs = [
    "OXYCODONE",
    "HYDROMORPHONE",
    "MEPERIDINE",
    "DIHYDROCODEINE",
    "TAPENTADOL",
    "CODEINE",
    "OXYMORPHONE",
    "MORPHINE",
    "HYDROCODONE",
    "LEVORPHANOL",
]

# Check if the file exists
if not os.path.exists(tsv_path):
    raise FileNotFoundError(f"File not found: {tsv_path}")

# Define the columns to use
usecols = [
    "BUYER_STATE",
    "BUYER_COUNTY",
    "CALC_BASE_WT_IN_GM",
    "MME",
    "TRANSACTION_DATE",
    "MME_Conversion_Factor",
    "DRUG_NAME",
]

# Read the data using Dask
print("Reading the data with Dask...")
ddf = dd.read_csv(tsv_path, sep="\t", usecols=usecols, dtype={"DRUG_NAME": "object"})

# Process the data
print("Processing the data...")

# Convert TRANSACTION_DATE to datetime and extract the year
ddf["TRANSACTION_DATE"] = dd.to_datetime(ddf["TRANSACTION_DATE"], errors="coerce")
ddf["YEAR"] = ddf["TRANSACTION_DATE"].dt.year

# Filter for specific drugs
ddf_filtered = ddf[ddf["DRUG_NAME"].isin(target_drugs)]

# Calculate strength
ddf_filtered["CALC_MME"] = (
    ddf_filtered["CALC_BASE_WT_IN_GM"] * ddf_filtered["MME_Conversion_Factor"]
)

# Group by the required columns and sum
grouped = (
    ddf_filtered.groupby(["YEAR", "BUYER_COUNTY", "BUYER_STATE"])
    .agg({"CALC_MME": "sum", "MME": "sum"})
    .reset_index()
)

# Rename columns to match the desired output
grouped = grouped.rename(
    columns={
        "BUYER_STATE": "STATE",
        "BUYER_COUNTY": "COUNTY",
    }
)

# Reorder columns to match the specified order
grouped = grouped[["YEAR", "STATE", "COUNTY", "CALC_MME", "MME"]]

# Persist the processed data to a single Parquet file using Dask
print("Saving the processed data to a single Parquet file...")
with ProgressBar():
    grouped.to_parquet(parquet_path, write_index=False, engine="pyarrow", compute=True)

print(f"Data saved to {parquet_path}")
