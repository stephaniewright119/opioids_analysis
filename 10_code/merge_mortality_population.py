import pandas as pd


def create_merged_df():

    # read in population data
    pop_df = pd.read_parquet("../00_data/population/population.parquet")
    # subset to the appropriate years
    pop_df = pop_df[(pop_df["Year"] > 2002) & (pop_df["Year"] < 2016)]

    # read in mortality data
    mort_df = pd.read_csv("../00_data/all_death_causes_2003_2015.csv")
    # subset to appropriate states
    mort_df = mort_df[mort_df["State"] != "AK"]
    mort_df = mort_df[mort_df["State"] != "CT"]

    # make strings so can appropriately be used as keys
    mort_df["FIPS_CODE"] = mort_df["FIPS_CODE"].astype(str)
    pop_df["FIPS_CODE"] = pop_df["FIPS_CODE"].astype(str)

    merged_df = pd.merge(
        pop_df,
        mort_df,
        how="left",
        on=["FIPS_CODE", "Year"],
        indicator=True,
    )

    merged_df = merged_df.drop(columns=["County", "State", "_merge"])

    return merged_df


if __name__ == "__main__":
    merged_df = create_merged_df()
