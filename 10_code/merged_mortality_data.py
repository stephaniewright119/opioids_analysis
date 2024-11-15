import pandas as pd

death_cause_2003 = pd.read_csv(
    "./00_data/US_VitalStatistics/death_cause_2003.txt",
    sep="\t",
)
death_cause_2003 = death_cause_2003.dropna(subset=["County"]).drop(
    columns=["Notes", "Year Code"]
)
death_cause_2003["County Code"] = death_cause_2003["County Code"].astype(int)
death_cause_2003["Year"] = death_cause_2003["Year"].astype(int)
death_cause_2003["Deaths"] = death_cause_2003["Deaths"].astype(int)
death_cause_2003["County Code"] = death_cause_2003["County Code"].apply(
    lambda x: f"{x:05d}"
)
death_cause_2003 = death_cause_2003[
    death_cause_2003["Drug/Alcohol Induced Cause"]
    == "Drug poisonings (overdose) Unintentional (X40-X44)"
]
death_cause_2003[["County", "State"]] = death_cause_2003["County"].str.split(
    ",", expand=True
)
columns_order = [
    "County",
    "State",
    "County Code",
    "Year",
    "Drug/Alcohol Induced Cause",
    "Drug/Alcohol Induced Cause Code",
    "Deaths",
]
death_cause_2003 = death_cause_2003[columns_order].drop(
    columns="Drug/Alcohol Induced Cause"
)

death_cause_2004 = pd.read_csv(
    "/Users/Cindy_ggjx/cindy-opioids-2024-drugzz/00_data/US_VitalStatistics/death_cause_2004.txt",
    sep="\t",
)
death_cause_2004 = death_cause_2004.dropna(subset=["County"]).drop(
    columns=["Notes", "Year Code"]
)
death_cause_2004["County Code"] = death_cause_2004["County Code"].astype(int)
death_cause_2004["Year"] = death_cause_2004["Year"].astype(int)
death_cause_2004["Deaths"] = death_cause_2004["Deaths"].astype(int)
death_cause_2004["County Code"] = death_cause_2004["County Code"].apply(
    lambda x: f"{x:05d}"
)
death_cause_2004 = death_cause_2004[
    death_cause_2004["Drug/Alcohol Induced Cause"]
    == "Drug poisonings (overdose) Unintentional (X40-X44)"
]
death_cause_2004[["County", "State"]] = death_cause_2004["County"].str.split(
    ",", expand=True
)
columns_order = [
    "County",
    "State",
    "County Code",
    "Year",
    "Drug/Alcohol Induced Cause",
    "Drug/Alcohol Induced Cause Code",
    "Deaths",
]
death_cause_2004 = death_cause_2004[columns_order].drop(
    columns="Drug/Alcohol Induced Cause"
)


death_cause_2005 = pd.read_csv(
    "/Users/Cindy_ggjx/cindy-opioids-2024-drugzz/00_data/US_VitalStatistics/death_cause_2005.txt",
    sep="\t",
)
death_cause_2005 = death_cause_2005.dropna(subset=["County"]).drop(
    columns=["Notes", "Year Code"]
)
death_cause_2005["County Code"] = death_cause_2005["County Code"].astype(int)
death_cause_2005["Year"] = death_cause_2005["Year"].astype(int)
death_cause_2005["Deaths"] = death_cause_2005["Deaths"].astype(int)
death_cause_2005["County Code"] = death_cause_2005["County Code"].apply(
    lambda x: f"{x:05d}"
)
death_cause_2005 = death_cause_2005[
    death_cause_2005["Drug/Alcohol Induced Cause"]
    == "Drug poisonings (overdose) Unintentional (X40-X44)"
]
death_cause_2005[["County", "State"]] = death_cause_2005["County"].str.split(
    ",", expand=True
)
columns_order = [
    "County",
    "State",
    "County Code",
    "Year",
    "Drug/Alcohol Induced Cause",
    "Drug/Alcohol Induced Cause Code",
    "Deaths",
]
death_cause_2005 = death_cause_2005[columns_order].drop(
    columns="Drug/Alcohol Induced Cause"
)


death_cause_2006 = pd.read_csv(
    "/Users/Cindy_ggjx/cindy-opioids-2024-drugzz/00_data/US_VitalStatistics/death_cause_2006.txt",
    sep="\t",
)
death_cause_2006 = death_cause_2006.dropna(subset=["County"]).drop(
    columns=["Notes", "Year Code"]
)


death_cause_2006["County Code"] = (
    death_cause_2006["County Code"].astype(int).apply(lambda x: f"{x:05d}")
)
death_cause_2006["Year"] = death_cause_2006["Year"].astype(int)
death_cause_2006["Deaths"] = death_cause_2006["Deaths"].astype(int)


death_cause_2006 = death_cause_2006[
    death_cause_2006["Drug/Alcohol Induced Cause"]
    == "Drug poisonings (overdose) Unintentional (X40-X44)"
]


death_cause_2006[["County", "State"]] = death_cause_2006["County"].str.split(
    ",", expand=True
)
death_cause_2006["State"] = death_cause_2006["State"].str.strip()


columns_order = [
    "County",
    "State",
    "County Code",
    "Year",
    "Drug/Alcohol Induced Cause",
    "Drug/Alcohol Induced Cause Code",
    "Deaths",
]
death_cause_2006 = death_cause_2006[columns_order].drop(
    columns="Drug/Alcohol Induced Cause"
)


death_cause_2007 = pd.read_csv(
    "/Users/Cindy_ggjx/cindy-opioids-2024-drugzz/00_data/US_VitalStatistics/death_cause_2007.txt",
    sep="\t",
)
death_cause_2007 = death_cause_2007.dropna(subset=["County"]).drop(
    columns=["Notes", "Year Code"]
)


death_cause_2007["County Code"] = (
    death_cause_2007["County Code"].astype(int).apply(lambda x: f"{x:05d}")
)
death_cause_2007["Year"] = death_cause_2007["Year"].astype(int)
death_cause_2007["Deaths"] = death_cause_2007["Deaths"].astype(int)


death_cause_2007 = death_cause_2007[
    death_cause_2007["Drug/Alcohol Induced Cause"]
    == "Drug poisonings (overdose) Unintentional (X40-X44)"
]


death_cause_2007[["County", "State"]] = death_cause_2007["County"].str.split(
    ",", expand=True
)
death_cause_2007["State"] = death_cause_2007["State"].str.strip()


columns_order = [
    "County",
    "State",
    "County Code",
    "Year",
    "Drug/Alcohol Induced Cause",
    "Drug/Alcohol Induced Cause Code",
    "Deaths",
]
death_cause_2007 = death_cause_2007[columns_order].drop(
    columns="Drug/Alcohol Induced Cause"
)

death_cause_2007

death_cause_2008 = pd.read_csv(
    "/Users/Cindy_ggjx/Desktop/pds project/US_VitalStatistics/death_cause_2008.txt",
    sep="\t",
)
death_cause_2008 = death_cause_2008.dropna(subset=["County"]).drop(
    columns=["Notes", "Year Code"]
)


death_cause_2008["County Code"] = (
    death_cause_2008["County Code"].astype(int).apply(lambda x: f"{x:05d}")
)
death_cause_2008["Year"] = death_cause_2008["Year"].astype(int)
death_cause_2008["Deaths"] = death_cause_2008["Deaths"].astype(int)


death_cause_2008 = death_cause_2008[
    death_cause_2008["Drug/Alcohol Induced Cause"]
    == "Drug poisonings (overdose) Unintentional (X40-X44)"
]


death_cause_2008[["County", "State"]] = death_cause_2008["County"].str.split(
    ",", expand=True
)
death_cause_2008["State"] = death_cause_2008["State"].str.strip()


columns_order = [
    "County",
    "State",
    "County Code",
    "Year",
    "Drug/Alcohol Induced Cause",
    "Drug/Alcohol Induced Cause Code",
    "Deaths",
]
death_cause_2008 = death_cause_2008[columns_order].drop(
    columns="Drug/Alcohol Induced Cause"
)


death_cause_2009 = pd.read_csv(
    "/Users/Cindy_ggjx/Desktop/pds project/US_VitalStatistics/death_cause_2009.txt",
    sep="\t",
)
death_cause_2009 = death_cause_2009.dropna(subset=["County"]).drop(
    columns=["Notes", "Year Code"]
)


death_cause_2009["County Code"] = (
    death_cause_2009["County Code"].astype(int).apply(lambda x: f"{x:05d}")
)
death_cause_2009["Year"] = death_cause_2009["Year"].astype(int)
death_cause_2009["Deaths"] = death_cause_2009["Deaths"].astype(int)


death_cause_2009 = death_cause_2009[
    death_cause_2009["Drug/Alcohol Induced Cause"]
    == "Drug poisonings (overdose) Unintentional (X40-X44)"
]


death_cause_2009[["County", "State"]] = death_cause_2009["County"].str.split(
    ",", expand=True
)
death_cause_2009["State"] = death_cause_2009["State"].str.strip()


columns_order = [
    "County",
    "State",
    "County Code",
    "Year",
    "Drug/Alcohol Induced Cause",
    "Drug/Alcohol Induced Cause Code",
    "Deaths",
]
death_cause_2009 = death_cause_2009[columns_order].drop(
    columns="Drug/Alcohol Induced Cause"
)

death_cause_2010 = pd.read_csv(
    "/Users/Cindy_ggjx/Desktop/pds project/US_VitalStatistics/death_cause_2010.txt",
    sep="\t",
)
death_cause_2010 = death_cause_2010.dropna(subset=["County"]).drop(
    columns=["Notes", "Year Code"]
)


death_cause_2010["County Code"] = (
    death_cause_2010["County Code"].astype(int).apply(lambda x: f"{x:05d}")
)
death_cause_2010["Year"] = death_cause_2010["Year"].astype(int)
death_cause_2010["Deaths"] = death_cause_2010["Deaths"].astype(int)


death_cause_2010 = death_cause_2010[
    death_cause_2010["Drug/Alcohol Induced Cause"]
    == "Drug poisonings (overdose) Unintentional (X40-X44)"
]


death_cause_2010[["County", "State"]] = death_cause_2010["County"].str.split(
    ",", expand=True
)
death_cause_2010["State"] = death_cause_2010["State"].str.strip()


columns_order = [
    "County",
    "State",
    "County Code",
    "Year",
    "Drug/Alcohol Induced Cause",
    "Drug/Alcohol Induced Cause Code",
    "Deaths",
]
death_cause_2010 = death_cause_2010[columns_order].drop(
    columns="Drug/Alcohol Induced Cause"
)

death_cause_2011 = pd.read_csv(
    "/Users/Cindy_ggjx/Desktop/pds project/US_VitalStatistics/death_cause_2011.txt",
    sep="\t",
)
death_cause_2011 = death_cause_2011.dropna(subset=["County"]).drop(
    columns=["Notes", "Year Code"]
)


death_cause_2011["County Code"] = (
    death_cause_2011["County Code"].astype(int).apply(lambda x: f"{x:05d}")
)
death_cause_2011["Year"] = death_cause_2011["Year"].astype(int)
death_cause_2011["Deaths"] = death_cause_2011["Deaths"].astype(int)


death_cause_2011 = death_cause_2011[
    death_cause_2011["Drug/Alcohol Induced Cause"]
    == "Drug poisonings (overdose) Unintentional (X40-X44)"
]


death_cause_2011[["County", "State"]] = death_cause_2011["County"].str.split(
    ",", expand=True
)
death_cause_2011["State"] = death_cause_2011["State"].str.strip()


columns_order = [
    "County",
    "State",
    "County Code",
    "Year",
    "Drug/Alcohol Induced Cause",
    "Drug/Alcohol Induced Cause Code",
    "Deaths",
]
death_cause_2011 = death_cause_2011[columns_order].drop(
    columns="Drug/Alcohol Induced Cause"
)

death_cause_2012 = pd.read_csv(
    "/Users/Cindy_ggjx/Desktop/pds project/US_VitalStatistics/death_cause_2012.txt",
    sep="\t",
)
death_cause_2012 = death_cause_2012.dropna(subset=["County"]).drop(
    columns=["Notes", "Year Code"]
)


death_cause_2012["County Code"] = (
    death_cause_2012["County Code"].astype(int).apply(lambda x: f"{x:05d}")
)
death_cause_2012["Year"] = death_cause_2012["Year"].astype(int)
death_cause_2012["Deaths"] = death_cause_2012["Deaths"].astype(int)


death_cause_2012 = death_cause_2012[
    death_cause_2012["Drug/Alcohol Induced Cause"]
    == "Drug poisonings (overdose) Unintentional (X40-X44)"
]


death_cause_2012[["County", "State"]] = death_cause_2012["County"].str.split(
    ",", expand=True
)
death_cause_2012["State"] = death_cause_2012["State"].str.strip()


columns_order = [
    "County",
    "State",
    "County Code",
    "Year",
    "Drug/Alcohol Induced Cause",
    "Drug/Alcohol Induced Cause Code",
    "Deaths",
]
death_cause_2012 = death_cause_2012[columns_order].drop(
    columns="Drug/Alcohol Induced Cause"
)

death_cause_2013 = pd.read_csv(
    "/Users/Cindy_ggjx/Desktop/pds project/US_VitalStatistics/death_cause_2013.txt",
    sep="\t",
)
death_cause_2013 = death_cause_2013.dropna(subset=["County"]).drop(
    columns=["Notes", "Year Code"]
)


death_cause_2013["County Code"] = (
    death_cause_2013["County Code"].astype(int).apply(lambda x: f"{x:05d}")
)
death_cause_2013["Year"] = death_cause_2013["Year"].astype(int)
death_cause_2013["Deaths"] = death_cause_2013["Deaths"].astype(int)


death_cause_2013 = death_cause_2013[
    death_cause_2013["Drug/Alcohol Induced Cause"]
    == "Drug poisonings (overdose) Unintentional (X40-X44)"
]


death_cause_2013[["County", "State"]] = death_cause_2013["County"].str.split(
    ",", expand=True
)
death_cause_2013["State"] = death_cause_2013["State"].str.strip()


columns_order = [
    "County",
    "State",
    "County Code",
    "Year",
    "Drug/Alcohol Induced Cause",
    "Drug/Alcohol Induced Cause Code",
    "Deaths",
]
death_cause_2013 = death_cause_2013[columns_order].drop(
    columns="Drug/Alcohol Induced Cause"
)

death_cause_2014 = pd.read_csv(
    "/Users/Cindy_ggjx/Desktop/pds project/US_VitalStatistics/death_cause_2014.txt",
    sep="\t",
)
death_cause_2014 = death_cause_2014.dropna(subset=["County"]).drop(
    columns=["Notes", "Year Code"]
)


death_cause_2014["County Code"] = (
    death_cause_2014["County Code"].astype(int).apply(lambda x: f"{x:05d}")
)
death_cause_2014["Year"] = death_cause_2014["Year"].astype(int)
death_cause_2014["Deaths"] = death_cause_2014["Deaths"].astype(int)


death_cause_2014 = death_cause_2014[
    death_cause_2014["Drug/Alcohol Induced Cause"]
    == "Drug poisonings (overdose) Unintentional (X40-X44)"
]


death_cause_2014[["County", "State"]] = death_cause_2014["County"].str.split(
    ",", expand=True
)
death_cause_2014["State"] = death_cause_2014["State"].str.strip()


columns_order = [
    "County",
    "State",
    "County Code",
    "Year",
    "Drug/Alcohol Induced Cause",
    "Drug/Alcohol Induced Cause Code",
    "Deaths",
]
death_cause_2014 = death_cause_2014[columns_order].drop(
    columns="Drug/Alcohol Induced Cause"
)

death_cause_2015 = pd.read_csv(
    "/Users/Cindy_ggjx/Desktop/pds project/US_VitalStatistics/death_cause_2015.txt",
    sep="\t",
)
death_cause_2015 = death_cause_2015.dropna(subset=["County"]).drop(
    columns=["Notes", "Year Code"]
)


death_cause_2015["County Code"] = (
    death_cause_2015["County Code"].astype(int).apply(lambda x: f"{x:05d}")
)
death_cause_2015["Year"] = death_cause_2015["Year"].astype(int)
death_cause_2015["Deaths"] = pd.to_numeric(
    death_cause_2015["Deaths"], errors="coerce"
).astype("Int64")


death_cause_2015 = death_cause_2015[
    death_cause_2015["Drug/Alcohol Induced Cause"]
    == "Drug poisonings (overdose) Unintentional (X40-X44)"
]


death_cause_2015[["County", "State"]] = death_cause_2015["County"].str.split(
    ",", expand=True
)
death_cause_2015["State"] = death_cause_2015["State"].str.strip()


columns_order = [
    "County",
    "State",
    "County Code",
    "Year",
    "Drug/Alcohol Induced Cause",
    "Drug/Alcohol Induced Cause Code",
    "Deaths",
]
death_cause_2015 = death_cause_2015[columns_order].drop(
    columns="Drug/Alcohol Induced Cause"
)

dataframes = [
    death_cause_2003,
    death_cause_2004,
    death_cause_2005,
    death_cause_2006,
    death_cause_2007,
    death_cause_2008,
    death_cause_2009,
    death_cause_2010,
    death_cause_2011,
    death_cause_2012,
    death_cause_2013,
    death_cause_2014,
    death_cause_2015,
]


death_causes_final = pd.concat(dataframes, ignore_index=True)

death_causes_final = death_causes_final.drop(columns="Drug/Alcohol Induced Cause Code")
death_causes_final.to_csv("all_death_causes_2003_2015.csv", index=False)
