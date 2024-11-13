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
    # print(FIPS_Codes_df.head())
    state_counts = FIPS_Codes_df.groupby("STATE").size()
    # print(state_counts)  # 3197
    print(len(FIPS_Codes_df))
    pd.set_option("display.max_rows", None)  # Show all rows
    print(FIPS_Codes_df[FIPS_Codes_df["STATE"] == "Virginia"]["COUNTYNAME"])

    # Optionally, reset the option if you want to return to default settings
    pd.reset_option("display.max_rows")

# STATE
# Alabama            67
# Arizona            15
# Arkansas           75
# California         58
# Colorado           64
# Delaware            3
# Florida            67
# Georgia           159
# Hawaii              5
# Idaho              44
# Illinois          102
# Indiana            92
# Iowa               99
# Kansas            105
# Kentucky          120
# Louisiana          64
# Maine              16
# Maryland           24
# Massachusetts      14
# Michigan           83
# Minnesota          87
# Mississippi        82
# Missouri          115
# Montana            56
# Nebraska           93
# Nevada             17
# New Hampshire      10
# New Jersey         21
# New Mexico         33
# New York           62
# North Carolina    100
# North Dakota       53
# Ohio               88
# Oklahoma           77
# Oregon             36
# Pennsylvania       67
# Rhode Island        5
# South Carolina     46
# South Dakota       66
# Tennessee          95
# Texas             254
# Utah               29
# Vermont            14
# ###########Virginia          133
# Washington         39
# West Virginia      55
# Wisconsin          72
# Wyoming            23
