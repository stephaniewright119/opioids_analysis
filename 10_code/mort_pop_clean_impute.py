import pandas as pd


def impute_and_save_deaths(input_path, output_path):
    # Read the input Parquet file
    mort_pop_unclean = pd.read_parquet(input_path, engine="fastparquet")

    # Identify rows with missing deaths
    missing_deaths_mask = mort_pop_unclean["Deaths"].isna()

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
    mortality_population_with_death_rate = mort_pop_unclean.merge(
        state_year_totals[["ST_NAME", "Year", "death_rate"]],
        on=["ST_NAME", "Year"],
        how="left",
    )

    # Print the number of rows with death count of 10 before imputing
    rows_with_deaths_10_before = (
        mortality_population_with_death_rate["Deaths"] == 10
    ).sum()
    print(
        f"Number of rows with death count of 10 before imputing: {rows_with_deaths_10_before}"
    )

    # Step 3: Impute missing deaths
    mortality_population_with_death_rate.loc[missing_deaths_mask, "Deaths"] = (
        mortality_population_with_death_rate.loc[missing_deaths_mask, "Population"]
        * mortality_population_with_death_rate.loc[missing_deaths_mask, "death_rate"]
    )

    # Step 4: Force imputed values greater than 10 to be 9.999999
    mortality_population_with_death_rate.loc[missing_deaths_mask, "Deaths"] = (
        mortality_population_with_death_rate.loc[missing_deaths_mask, "Deaths"].apply(
            lambda x: 9.999999 if x >= 10 else x
        )
    )

    # Print the number of rows imputed to have deaths >= 10
    imputed_rows_above_10 = (
        mortality_population_with_death_rate.loc[missing_deaths_mask, "Deaths"] >= 10
    ).sum()
    print(f"Number of rows imputed to have deaths >= 10: {imputed_rows_above_10}")

    # Print the number of rows with death count of 10 after imputing
    rows_with_deaths_10_after = (
        mortality_population_with_death_rate["Deaths"] == 10
    ).sum()
    print(
        f"Number of rows with death count of 10 after imputing: {rows_with_deaths_10_after}"
    )

    # Save the resulting dataset to the output path
    mortality_population_with_death_rate.to_parquet(output_path, index=False)


if __name__ == "__main__":
    # Define input and output file paths
    input_file = "./20_intermediate_files/mort_pop_merge_unclean.parquet"
    output_file = "./20_intermediate_files/mort_pop_cleaned_imputed.parquet"

    # Run the function
    impute_and_save_deaths(input_file, output_file)
