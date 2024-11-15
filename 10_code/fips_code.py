import pandas as pd
import numpy as np
import os


def make_fips_df():
    fips_df = pd.read_csv(
        "https://www2.census.gov/geo/docs/reference/codes2020/national_county2020.txt",
        delimiter="|",
    )
    fips_df = fips_df[~fips_df["STATE"].isin(["AK", "CT"])]

    fips_df["STATEFP"] = fips_df["STATEFP"].astype(str).str.zfill(2)
    fips_df["COUNTYFP"] = fips_df["COUNTYFP"].astype(str).str.zfill(3)
    fips_df["FIPS_CODE"] = fips_df["STATEFP"] + fips_df["COUNTYFP"]
    fips_df = fips_df[["STATE", "COUNTYNAME", "FIPS_CODE"]]

    # changing state_county_FIPS ['STATE'] to be the spelled out instead of abbreviation
    # Create a dictionary to map state abbreviations to full names
    state_abbreviation_to_full = {
        "AL": "Alabama",
        "AK": "Alaska",
        "AZ": "Arizona",
        "AR": "Arkansas",
        "CA": "California",
        "CO": "Colorado",
        "CT": "Connecticut",
        "DE": "Delaware",
        "FL": "Florida",
        "GA": "Georgia",
        "HI": "Hawaii",
        "ID": "Idaho",
        "IL": "Illinois",
        "IN": "Indiana",
        "IA": "Iowa",
        "KS": "Kansas",
        "KY": "Kentucky",
        "LA": "Louisiana",
        "ME": "Maine",
        "MD": "Maryland",
        "MA": "Massachusetts",
        "MI": "Michigan",
        "MN": "Minnesota",
        "MS": "Mississippi",
        "MO": "Missouri",
        "MT": "Montana",
        "NE": "Nebraska",
        "NV": "Nevada",
        "NH": "New Hampshire",
        "NJ": "New Jersey",
        "NM": "New Mexico",
        "NY": "New York",
        "NC": "North Carolina",
        "ND": "North Dakota",
        "OH": "Ohio",
        "OK": "Oklahoma",
        "OR": "Oregon",
        "PA": "Pennsylvania",
        "RI": "Rhode Island",
        "SC": "South Carolina",
        "SD": "South Dakota",
        "TN": "Tennessee",
        "TX": "Texas",
        "UT": "Utah",
        "VT": "Vermont",
        "VA": "Virginia",
        "WA": "Washington",
        "WV": "West Virginia",
        "WI": "Wisconsin",
        "WY": "Wyoming",
    }

    # Map the abbreviations to full names
    fips_df["STATE"] = fips_df["STATE"].map(state_abbreviation_to_full)

    # Display the first few rows to verify the transformation
    fips_df.head(10)
    return fips_df


if __name__ == "__main__":
    FIPS_Codes_df = make_fips_df()
    FIPS_Codes_df.to_parquet("./00_data/population/fips_codes.parquet")
