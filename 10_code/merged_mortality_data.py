import pandas as pd


# Function to process individual death cause files
def process_death_cause_file(filepath, valid_codes=["D1", "D9", "D4"]):
    """
    Processes a single death cause file and returns a cleaned DataFrame.
    """
    df = pd.read_csv(filepath, sep="\t")

    # Drop rows with missing County and unnecessary columns
    df = df.dropna(subset=["County"]).drop(columns=["Notes", "Year Code"])

    # Convert columns to appropriate types
    df["County Code"] = df["County Code"].astype(int).apply(lambda x: f"{x:05d}")
    df["Year"] = df["Year"].astype(int)
    df["Deaths"] = pd.to_numeric(df["Deaths"], errors="coerce").astype("Int64")

    # Filter by valid cause codes
    df = df[df["Drug/Alcohol Induced Cause Code"].isin(valid_codes)]

    # Split County into County and State
    df[["County", "State"]] = df["County"].str.split(",", expand=True)
    df["State"] = df["State"].str.strip()

    # Reorder and clean columns
    columns_order = ["County", "State", "County Code", "Year", "Deaths"]
    df = df[columns_order]

    # rename column to be consistent with other datasets
    df.rename(columns={"County Code": "FIPS_CODE"}, inplace=True)

    # fips codes need to be 5 digits.
    # appending a 0 to the front of those that are not 5 digits long
    df["FIPS_CODE"] = df["FIPS_CODE"].astype(str).str.zfill(5)

    return df


# Base file path with year placeholder
base_filepath = "./00_data/US_VitalStatistics/death_cause_{}.txt"

# List of years to process
years = range(2003, 2016)  # From 2003 to 2015

# Process files for each year
dataframes = [process_death_cause_file(base_filepath.format(year)) for year in years]

# Combine all years into a single DataFrame
death_causes_final = pd.concat(dataframes, ignore_index=True)

print(death_causes_final.head(10))
# Save the final DataFrame to CSV
death_causes_final.to_parquet(
    "./00_data/all_death_causes_2003_2015.parquet", index=False
)
