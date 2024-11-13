import pandas as pd
import numpy as np
import os


def create_pop_data():
    popdata_til2010 = pd.read_csv("popestimate_2000_2010.csv", encoding="ISO-8859-1")
    popdata_after2010 = pd.read_csv("popestimate_2010_2019.csv", encoding="ISO-8859-1")

    popdata_til2010 = subset_pop2000_cols(popdata_til2010)
    popdata_after2010 = subset_pop2010_cols(popdata_after2010)

    popdata_til2010 = remove_states(popdata_til2010)
    popdata_after2010 = remove_states(popdata_after2010)

    # merge datasets
    population_data = combine_datasets(
        popdata_til2010, popdata_after2010, ["STATE", "COUNTY"]
    )
    # print(population_data.head())
    # print(population_data.columns)

    population_data = population_data[
        population_data.columns[~population_data.columns.str.endswith("_y")]
    ]
    print(population_data.head())

    population_data = create_FIPS_col(population_data)
    population_data = rename_cols_better(population_data)
    population_data = drop_stateonly_rows(population_data)
    return population_data


def drop_stateonly_rows(dataset):
    return dataset[dataset["ST_NAME"] != dataset["CTY_NAME"]]


def remove_states(dataset):
    dataset = dataset[~dataset["STNAME"].isin(["Alaska", "Connecticut"])]
    return dataset


def subset_pop2000_cols(dataset):
    dataset = dataset[
        [
            "REGION",
            "DIVISION",
            "STATE",
            "COUNTY",
            "STNAME",
            "CTYNAME",
            "POPESTIMATE2000",
            "POPESTIMATE2001",
            "POPESTIMATE2002",
            "POPESTIMATE2003",
            "POPESTIMATE2004",
            "POPESTIMATE2005",
            "POPESTIMATE2006",
            "POPESTIMATE2007",
            "POPESTIMATE2008",
        ]
    ]
    return dataset


def subset_pop2010_cols(dataset):
    # subset to only include relevant columns

    # way to generalize?
    dataset = dataset[
        [
            "REGION",
            "DIVISION",
            "STATE",
            "COUNTY",
            "STNAME",
            "CTYNAME",
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
    return dataset


def combine_datasets(data2000, data2010, merge_key):
    new_dataset = pd.merge(
        data2000, data2010, on=merge_key, how="outer", validate="1:1", indicator=True
    )
    # print(new_dataset["_merge"].value_counts())
    # print(
    #     new_dataset[
    #         (new_dataset._merge == "left_only") | (new_dataset._merge == "right_only")
    #     ].sort_values("COUNTY")
    # )
    return new_dataset


def create_FIPS_col(population_data):
    population_data["STATE"] = population_data["STATE"].astype(str).str.zfill(2)
    population_data["COUNTY"] = population_data["COUNTY"].astype(str).str.zfill(3)
    population_data["FIPS_CODE"] = population_data["STATE"] + population_data["COUNTY"]
    return population_data


def rename_cols_better(population_data):
    col_dict = {
        "REGION_x": "REGION",
        "STNAME_x": "ST_NAME",
        "CTYNAME_x": "CTY_NAME",
        "DIVISION_x": "DIVISION",
    }
    population_data = population_data.rename(columns=col_dict)
    # also going to move the FIPS code column to be the first column
    column_to_front = "FIPS_CODE"
    columns = [column_to_front] + [
        col for col in population_data.columns if col != column_to_front
    ]
    population_data = population_data[columns]
    return population_data


# ------------------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    population_data = create_pop_data()
    # print(len(population_data))
    # print(population_data.head())
    # print(population_data.columns)
    # making a separate df for FIPS codes as we might need it for something else
    state_counts = population_data.groupby("ST_NAME").size()
    # print(state_counts)
    print(len(population_data))  # 3106
    pd.set_option("display.max_rows", None)
    print(population_data[population_data["ST_NAME"] == "Virginia"]["CTY_NAME"])
    pd.reset_option("display.max_rows")
