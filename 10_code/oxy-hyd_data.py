import pandas as pd

prescription_subset_df = pd.read_parquet(
    "./20_intermediate_files/prescription_subset_oxy_hydro.parquet",
    engine="fastparquet",
)

prescription_subset_df.head()


popu_df = pd.read_parquet("00_data/population/population.parquet")
popu_df


state_map = {
    "ALABAMA": "AL",
    "ALASKA": "AK",
    "ARIZONA": "AZ",
    "ARKANSAS": "AR",
    "CALIFORNIA": "CA",
    "COLORADO": "CO",
    "CONNECTICUT": "CT",
    "DELAWARE": "DE",
    "FLORIDA": "FL",
    "GEORGIA": "GA",
    "HAWAII": "HI",
    "IDAHO": "ID",
    "ILLINOIS": "IL",
    "INDIANA": "IN",
    "IOWA": "IA",
    "KANSAS": "KS",
    "KENTUCKY": "KY",
    "LOUISIANA": "LA",
    "MAINE": "ME",
    "MARYLAND": "MD",
    "MASSACHUSETTS": "MA",
    "MICHIGAN": "MI",
    "MINNESOTA": "MN",
    "MISSISSIPPI": "MS",
    "MISSOURI": "MO",
    "MONTANA": "MT",
    "NEBRASKA": "NE",
    "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH",
    "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM",
    "NEW YORK": "NY",
    "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND",
    "OHIO": "OH",
    "OKLAHOMA": "OK",
    "OREGON": "OR",
    "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI",
    "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN",
    "TEXAS": "TX",
    "UTAH": "UT",
    "VERMONT": "VT",
    "VIRGINIA": "VA",
    "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI",
    "WYOMING": "WY",
}


popu_df["ST_NAME"] = popu_df["ST_NAME"].str.upper().map(state_map)

popu_df["CTY_NAME"] = (
    popu_df["CTY_NAME"].str.replace(" County", "", regex=False).str.strip().str.upper()
)


prescription_subset_df["COUNTY"] = (
    prescription_subset_df["COUNTY"].str.strip().str.upper()
)

# We noticed there was discrpency in county names between the two datasets, updating here
# Alabama
popu_df.loc[popu_df["ST_NAME"] == "Alabama", "CTY_NAME"] = popu_df.loc[
    popu_df["ST_NAME"] == "Alabama", "CTY_NAME"
].replace("ST. CLAIR", "SAINT CLAIR")
popu_df.loc[popu_df["ST_NAME"] == "Alabama", "CTY_NAME"] = popu_df.loc[
    popu_df["ST_NAME"] == "Alabama", "CTY_NAME"
].replace("DEKALB", "DE KALB")

# Michigan
popu_df.loc[popu_df["ST_NAME"] == "Michigan", "CTY_NAME"] = popu_df.loc[
    popu_df["ST_NAME"] == "Michigan", "CTY_NAME"
].replace("ST. CLAIR", "SAINT CLAIR")
popu_df.loc[popu_df["ST_NAME"] == "Michigan", "CTY_NAME"] = popu_df.loc[
    popu_df["ST_NAME"] == "Michigan", "CTY_NAME"
].replace("ST. JOSEPH", "SAINT JOSEPH")

# Florida
popu_df.loc[popu_df["ST_NAME"] == "Florida", "CTY_NAME"] = popu_df.loc[
    popu_df["ST_NAME"] == "Florida", "CTY_NAME"
].replace("DESOTO", "DE SOTO")
popu_df.loc[popu_df["ST_NAME"] == "Florida", "CTY_NAME"] = popu_df.loc[
    popu_df["ST_NAME"] == "Florida", "CTY_NAME"
].replace("SAINT LUCIE", "ST. LUCIE")
popu_df.loc[popu_df["ST_NAME"] == "Florida", "CTY_NAME"] = popu_df.loc[
    popu_df["ST_NAME"] == "Florida", "CTY_NAME"
].replace("SAINT JOHNS", "ST. JOHNS")

# Merge the datasets
merged_df = pd.merge(
    prescription_subset_df,
    popu_df,
    left_on=["STATE", "COUNTY", "YEAR"],
    right_on=["ST_NAME", "CTY_NAME", "Year"],
    how="left",
)

# Keep only the relevant columns
merged_df = merged_df[
    [
        "STATE",
        "COUNTY",
        "YEAR",
        "CALC_MME",
        "MME",
        "FIPS_CODE",
        "Population",
    ]
]
merged_df = merged_df.dropna(subset=["FIPS_CODE", "Population"])

merged_df["FIPS_CODE"] = merged_df["FIPS_CODE"].astype(int)
merged_df["Population"] = merged_df["Population"].astype(int)

merged_df.to_parquet("20_intermediate_files/oxy_hyd_data.parquet", index=False)
