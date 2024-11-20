import pandas as pd

prescription_subset_df = pd.read_parquet(
    "./20_intermediate_files/prescription_subset.parquet", engine="fastparquet"
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


prescription_subset_df["BUYER_COUNTY"] = (
    prescription_subset_df["BUYER_COUNTY"].str.strip().str.upper()
)


# Merge the datasets
merged_df = pd.merge(
    prescription_subset_df,
    popu_df,
    left_on=["BUYER_STATE", "BUYER_COUNTY", "year"],
    right_on=["ST_NAME", "CTY_NAME", "Year"],
    how="left",
)

# Keep only the relevant columns
merged_df = merged_df[
    [
        "BUYER_STATE",
        "BUYER_COUNTY",
        "year",
        "MME_Conversion_Factor",
        "CALC_BASE_WT_IN_GM",
        "strength",
        "FIPS_CODE",
        "Population",
    ]
]
merged_df = merged_df.dropna(subset=["FIPS_CODE", "Population"])

merged_df["FIPS_CODE"] = merged_df["FIPS_CODE"].astype(int)
merged_df["Population"] = merged_df["Population"].astype(int)

wa_states = ["WA", "OR", "CA", "CO", "MT"]
wa_controls_prescrip = merged_df[merged_df["BUYER_STATE"].isin(wa_states)]
wa_controls_prescrip.to_parquet(
    "20_intermediate_files/wa_controls_prescrip.parquet", index=False
)

fl_states = ["FL", "GA", "SC", "NC", "AL"]
fl_controls_prescrip = merged_df[merged_df["BUYER_STATE"].isin(fl_states)]
fl_controls_prescrip.to_parquet(
    "20_intermediate_files/fl_controls_prescrip.parquet", index=False
)
