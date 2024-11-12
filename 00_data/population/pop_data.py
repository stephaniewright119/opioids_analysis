import pandas as pd
import numpy as np
import os

print(os.getcwd())


def combine_datasets(data2000, data2010, merge_key):
    new_dataset = pd.merge(data2000, data2010, on=merge_key)
    return new_dataset


def create_FIPS_col(population_data):
    population_data["STATE"] = population_data["STATE"].astype(str).str.zfill(2)
    population_data["COUNTY"] = population_data["COUNTY"].astype(str).str.zfill(3)
    population_data["FIPS_CODE"] = population_data["STATE"] + population_data["COUNTY"]
    return population_data

def rename_cols_better(population_data):

def make_fips_df():
    fips_df = pd.read_csv(
        "https://www2.census.gov/geo/docs/reference/codes2020/national_county2020.txt",
        delimiter="|",
    )
    fips_df = fips_df[["STATE", "STATEFP", "COUNTYFP", "COUNTYNAME"]]
    return fips_df


if __name__ == "__main__":
    popdata_til2010 = pd.read_csv(
        "/Users/kaylahaeusssler/Documents/PDS/PDS_Final_opioids-2024/00_data/population/popestimate_2000_2010.csv",
        encoding="ISO-8859-1",
    )
    popdata_after2010 = pd.read_csv(
        "/Users/kaylahaeusssler/Documents/PDS/PDS_Final_opioids-2024/00_data/population/popestimate_2010_2019.csv",
        encoding="ISO-8859-1",
    )

    # this dataset had a bunch of other columns that were not relevant
    # so only including those which are similar to what we have in the pre-2010 dataset
    popdata_after2010 = popdata_after2010[
        [
            "SUMLEV",
            "REGION",
            "DIVISION",
            "STATE",
            "COUNTY",
            "STNAME",
            "CTYNAME",
            "CENSUS2010POP",
            "ESTIMATESBASE2010",
            "POPESTIMATE2010",
            "POPESTIMATE2011",
            "POPESTIMATE2012",
            "POPESTIMATE2013",
            "POPESTIMATE2014",
            "POPESTIMATE2015",
            "POPESTIMATE2016",
            "POPESTIMATE2017",
            "POPESTIMATE2018",
        ]
    ]
    # print(popdata_til2010.head())
    # print(popdata_after2010.head())
    # print(state_county_FIPS.head())

    # now we merge the two!
    population_data = combine_datasets(
        popdata_til2010, popdata_after2010, ["STATE", "COUNTY"]
    )
    population_data = population_data.drop(columns=["SUMLEV_x", "DIVISION_x"], axis=1)
    print(population_data.head())

    population_data = create_FIPS_col(population_data)
    print(population_data.head())

    population_data = rename_cols_better(population_data)

    # making a separate df for FIPS codes as we might need it for something else
    FIPS_Codes_df = make_fips_df()
