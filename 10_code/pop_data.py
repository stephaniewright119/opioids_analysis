import pandas as pd
import numpy as np
import os


def create_pop_data():
    popdata_til2010 = pd.read_csv(
        "./00_data/population/popestimate_2000_2010.csv", encoding="ISO-8859-1"
    )
    popdata_after2010 = pd.read_csv(
        "./00_data/population/popestimate_2010_2019.csv", encoding="ISO-8859-1"
    )

    popdata_til2010 = subset_pop2000_cols(popdata_til2010)
    # the original post 2010 data set has 164 columns, so reducing those to only columns of interest
    #       columns selected are similar to the 15 cols of interest we selected for pre 2010 data above
    popdata_after2010 = subset_pop2010_cols(popdata_after2010)

    # removing Alaska and Conneticut as they have atypical counties
    popdata_til2010 = remove_states(popdata_til2010)
    popdata_after2010 = remove_states(popdata_after2010)

    # merge datasets
    population_data = combine_datasets(
        popdata_til2010, popdata_after2010, ["STATE", "COUNTY"]
    )

    # after we merge the data, the columns that appeared in both datasets have a '_x' or '_y'
    #       at the end of the column name. since these columns are duplicates we drop those that
    #       end in '_y'
    population_data = population_data[
        population_data.columns[~population_data.columns.str.endswith("_y")]
    ]

    # drop the column we created earlier to check our merge
    population_data = population_data.drop("_merge", axis=1)

    # create FIPS code column
    population_data = create_FIPS_col(population_data)

    # give our columns more intuitive names
    population_data = rename_cols_better(population_data)

    # there are rows in the data set where the state and county name are both the state name
    #       these rows show total state population data. this is not something we are interested in
    #       so we drop these rows
    population_data = drop_stateonly_rows(population_data)

    return population_data


def drop_stateonly_rows(dataset):
    """drop rows of state total population as we are only interested in county data"""
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
        data2000, data2010, on=merge_key, how="inner", validate="1:1", indicator=True
    )
    print(new_dataset["_merge"].value_counts())
    print(
        new_dataset[
            (new_dataset._merge == "left_only") | (new_dataset._merge == "right_only")
        ].sort_values("COUNTY")
    )
    return new_dataset


def create_FIPS_col(population_data):
    population_data["STATE"] = population_data["STATE"].astype(str).str.zfill(2)
    population_data["COUNTY"] = population_data["COUNTY"].astype(str).str.zfill(3)
    population_data["FIPS_CODE"] = population_data["STATE"] + population_data["COUNTY"]

    # now that we have used the STATE and COUNTY columns to construct the FIPS code,
    #       it is not necessary to keep them, so dropping them now
    population_data = population_data.drop(["STATE", "COUNTY"], axis=1)
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


def melt_df(pop_df):
    # Reshape the DataFrame
    pop_df_melted = pop_df.melt(
        id_vars=[
            "FIPS_CODE",
            "REGION",
            "DIVISION",
            "ST_NAME",
            "CTY_NAME",
        ],  # Keep these columns as identifiers
        var_name="Year",  # Name of the new column that will hold the original column names
        value_name="Population",  # Name of the new column that will hold the values
    )
    # Convert the "Year" column to an integer by removing the 'POPESTIMATE' prefix
    pop_df_melted["Year"] = (
        pop_df_melted["Year"].str.replace("POPESTIMATE", "").astype(int)
    )

    return pop_df_melted


# ------------------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    population_data = create_pop_data()
    print(population_data.head())
    population_data.to_csv("population_wide.csv", index=False)

    # we now will reshape the data to get it in a shape that is easier for merging with our other
    #   datasets for this project
    melted_population_df = melt_df(population_data)
    melted_population_df.to_csv("population.csv", index=False)
    melted_population_df.to_parquet("population.parquet")
