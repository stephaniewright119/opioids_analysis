import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import os

# Unique drug names:
# OXYCODONE
# HYDROMORPHONE
# METHADONE
# MEPERIDINE
# DIHYDROCODEINE
# TAPENTADOL
# CODEINE
# OXYMORPHONE
# MORPHINE
# OPIUM, POWDERED
# HYDROCODONE
# FENTANYL
# BUPRENORPHINE
# LEVORPHANOL

# File paths
tsv_path = "./00_data/arcos_all.tsv"
parquet_path = "./20_intermediate_files/prescription_subset.parquet"

# Check if the file exists
if not os.path.exists(tsv_path):
    raise FileNotFoundError(f"File not found: {tsv_path}")

# Define the columns to use
usecols = [
    "BUYER_STATE",
    "BUYER_COUNTY",
    "CALC_BASE_WT_IN_GM",
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
ddf["year"] = ddf["TRANSACTION_DATE"].dt.year

# Filter for specific drugs
target_drugs = ["HYDROCODONE", "OXYCODONE", "OXYMORPHONE", "MORPHINE", "CODEINE"]
ddf_filtered = ddf[ddf["DRUG_NAME"].isin(target_drugs)]

# Calculate strength
ddf_filtered["strength"] = (
    ddf_filtered["CALC_BASE_WT_IN_GM"] * ddf_filtered["MME_Conversion_Factor"]
)

# Group by the required columns and sum
grouped = ddf_filtered.groupby(["year", "BUYER_COUNTY", "BUYER_STATE"]).sum(
    numeric_only=True
)

# Reset the index
grouped = grouped.reset_index()

# Persist the processed data to a Parquet file
print("Saving the processed data to a Parquet file...")
with ProgressBar():
    grouped.to_parquet(parquet_path, write_index=False, engine="pyarrow")
print(f"Data saved to {parquet_path}")
