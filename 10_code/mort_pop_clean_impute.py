import pandas as pd


def impute_and_save_deaths(input_path, output_path):
    # Read the input Parquet file
    mort_pop_unclean = pd.read_parquet(input_path, engine="fastparquet")

    # Identify rows with missing deaths
    counties_missing_deaths = mort_pop_unclean[mort_pop_unclean["Deaths"].isna()]

    # Step 1: Calculate state-year death rate
    state_year_totals = (
        mort_pop_unclean.groupby(["ST_NAME", "Year"])
        .agg(total_deaths=("Deaths", "sum"), total_population=("Population", "sum"))
        .reset_index()
    )

    # Add death rate column
    state_year_totals["death_rate"] = (
        state_year_totals["total_deaths"] / state_year_totals["total_population"]
    )

    # Step 2: Merge death rate back to the original dataset
    mort_pop_no_missing = mort_pop_unclean.merge(
        state_year_totals[["ST_NAME", "Year", "death_rate"]],
        on=["ST_NAME", "Year"],
        how="left",
    )

    # Step 3: Impute missing deaths
    mort_pop_no_missing["Deaths"] = mort_pop_no_missing["Deaths"].fillna(
        mort_pop_no_missing["Population"] * mort_pop_no_missing["death_rate"]
    )

    # Step 4: Round deaths to the nearest integer
    mort_pop_no_missing["Deaths"] = mort_pop_no_missing["Deaths"].round()

    # Step 5: Force imputed values greater than 10 to be 9
    mort_pop_no_missing["Deaths"] = mort_pop_no_missing["Deaths"].apply(
        lambda x: 9 if pd.isna(x) is False and x > 10 else x
    )

    # Save the resulting dataset to the output path
    mort_pop_no_missing.to_parquet(output_path, index=False)


if __name__ == "__main__":
    # Define input and output file paths
    input_file = "./20_intermediate_files/mort_pop_merge_unclean.parquet"
    output_file = "./20_intermediate_files/mort_pop_cleaned_imputed.parquet"

    # Run the function
    impute_and_save_deaths(input_file, output_file)
