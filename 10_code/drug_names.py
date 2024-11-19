import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import os

# File path for the TSV file
tsv_path = "./00_data/arcos_all.tsv"

# Check if the file exists
if not os.path.exists(tsv_path):
    raise FileNotFoundError(f"File not found: {tsv_path}")

# Load the TSV file using Dask
print("Loading the TSV file...")
ddf = dd.read_csv(tsv_path, sep="\t", dtype={"DRUG_NAME": "object"})

# Check if DRUG_NAME column exists
if "DRUG_NAME" not in ddf.columns:
    raise KeyError("The column 'DRUG_NAME' does not exist in the dataset.")

# Get unique values of the DRUG_NAME column with a progress bar
print("Retrieving unique values from the 'DRUG_NAME' column...")
with ProgressBar():
    unique_drug_names = ddf["DRUG_NAME"].dropna().unique().compute()

# Print unique values
print("Unique drug names:")
for drug in unique_drug_names:
    print(drug)

# This takes a very long time to run
# Here are the results

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
