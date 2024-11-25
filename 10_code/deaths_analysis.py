# necessary packages

import pandas as pd
import numpy as np
import seaborn.objects as so
import seaborn as sns
import matplotlib.pyplot as plt

pd.set_option("mode.copy_on_write", True)

# datasets

deaths = pd.read_parquet("20_intermediate_files/mort_pop_cleaned_imputed.parquet")

deaths["death_per_100k"] = deaths["Deaths"] / deaths["Population"] * 100000

# pre-post datasets (without controls)

wa_deaths_pp = deaths[deaths["ST_NAME"] == "Washington"]
wa_deaths_pp["post"] = np.where(wa_deaths_pp["Year"] > 2011, 1, 0)

fl_deaths_pp = deaths[deaths["ST_NAME"] == "Florida"]
fl_deaths_pp["post"] = np.where(fl_deaths_pp["Year"] > 2009, 1, 0)

# diff in diff datasets (with controls)

wa_deaths_did = deaths[
    deaths["ST_NAME"].isin(
        ["Washington", "Oregon", "Colorado", "Montana"]
    )
]

wa_deaths_did["DID"] = np.where(
    (wa_deaths_did["ST_NAME"] == "Washington") & (wa_deaths_did["Year"] < 2012),
    1,
    np.where(
        (wa_deaths_did["ST_NAME"] != "Washington") & (wa_deaths_did["Year"] < 2012),
        2,
        np.where(
            (wa_deaths_did["ST_NAME"] == "Washington") & (wa_deaths_did["Year"] > 2011),
            3,
            np.where(
                (wa_deaths_did["ST_NAME"] != "Washington")
                & (wa_deaths_did["Year"] > 2011),
                4,
                np.nan,
            ),
        ),
    ),
)

fl_deaths_did = deaths[
    deaths["ST_NAME"].isin(
        ["Florida", "Georgia", "South Carolina", "North Carolina", "Alabama"]
    )
]

fl_deaths_did["DID"] = np.where(
    (fl_deaths_did["ST_NAME"] == "Florida") & (fl_deaths_did["Year"] < 2010),
    1,
    np.where(
        (fl_deaths_did["ST_NAME"] != "Florida") & (fl_deaths_did["Year"] < 2010),
        2,
        np.where(
            (fl_deaths_did["ST_NAME"] == "Florida") & (fl_deaths_did["Year"] > 2009),
            3,
            np.where(
                (fl_deaths_did["ST_NAME"] != "Florida")
                & (fl_deaths_did["Year"] > 2009),
                4,
                np.nan,
            ),
        ),
    ),
)

# Washington Deaths Pre-Post Plot

wa_deaths_pp_plot = sns.lmplot(
    data=wa_deaths_pp,
    x="Year",
    y="death_per_100k",
    hue="post",
    ci=95,
    scatter=False,
    line_kws={"lw": 2},
    legend=False,
    aspect=1.5,
)

ax = wa_deaths_pp_plot.ax

ax.axvline(x=2012, color="black", linestyle="--", linewidth=1, label="Policy Change")

ax.set_title("Washington Deaths Per 100,000 (Pre vs. Post Policy)")
ax.set_xlabel("Year")
ax.set_ylabel("Deaths per 100,000")

custom_lines = [
    plt.Line2D([0], [0], color=sns.color_palette()[0], lw=2, label="Pre-Policy"),
    plt.Line2D([0], [0], color=sns.color_palette()[1], lw=2, label="Post-Policy"),
    plt.Line2D([0], [0], color="black", linestyle="--", lw=1, label="Policy Change"),
]
ax.legend(handles=custom_lines, loc="upper left")

plt.savefig("30_results/wa_deaths_pp_plot.png", dpi=300, bbox_inches="tight")

# Florida Deaths Pre-Post Plot

fl_deaths_pp_plot = sns.lmplot(
    data=fl_deaths_pp,
    x="Year",
    y="death_per_100k",
    hue="post",
    ci=95,
    scatter=False,
    line_kws={"lw": 2},
    legend=False,
    aspect=1.5,
)

ax = fl_deaths_pp_plot.ax

ax.axvline(x=2010, color="black", linestyle="--", linewidth=1, label="Policy Change")

plt.title("Florida Deaths Per 100,000 (Pre vs. Post Policy)")
plt.xlabel("Year")
plt.ylabel("Deaths per 100,000")

custom_lines = [
    plt.Line2D([0], [0], color=sns.color_palette()[0], lw=2, label="Pre-Policy"),
    plt.Line2D([0], [0], color=sns.color_palette()[1], lw=2, label="Post-Policy"),
    plt.Line2D([0], [0], color="black", linestyle="--", lw=1, label="Policy Change"),
]
ax.legend(handles=custom_lines, loc="upper left")

plt.savefig("30_results/fl_deaths_pp_plot.png", dpi=300, bbox_inches="tight")

# Washington Deaths Diff-in-Diff Plot

wa_deaths_did_plot = sns.lmplot(
    data=wa_deaths_did,
    x="Year",
    y="death_per_100k",
    hue="DID",
    palette={1: "blue", 2: "orange", 3: "blue", 4: "orange"},
    ci=95,
    scatter=False,
    line_kws={"lw": 2},
    legend=False,
    aspect=1.5,
)

ax = wa_deaths_did_plot.ax

plt.axvline(x=2012, color="black", linestyle="--", linewidth=1, label="Policy Change")

plt.title("Opioid Deaths Per 100,000 (Pre vs. Post Policy)")
plt.xlabel("Year")
plt.ylabel("Opioid Deaths per 100,000 People")

custom_lines = [
    plt.Line2D([0], [0], color=sns.color_palette()[0], lw=2, label="Washington"),
    plt.Line2D([0], [0], color=sns.color_palette()[1], lw=2, label="Controls"),
    plt.Line2D([0], [0], color="black", linestyle="--", lw=1, label="Policy Change"),
]
ax.legend(handles=custom_lines, loc="upper left")

plt.savefig("30_results/wa_deaths_did_plot.png", dpi=300, bbox_inches="tight")

# Florida Deaths Diff-in-Diff Plot

fl_deaths_did_plot = sns.lmplot(
    data=fl_deaths_did,
    x="Year",
    y="death_per_100k",
    hue="DID",
    palette={1: "blue", 2: "orange", 3: "blue", 4: "orange"},
    ci=95,
    scatter=False,
    line_kws={"lw": 2},
    legend=False,
    aspect=1.5,
)

ax = fl_deaths_did_plot.ax

plt.axvline(x=2010, color="black", linestyle="--", linewidth=1, label="Policy Change")

plt.title("Opioid Deaths Per 100,000 (Pre vs. Post Policy)")
plt.xlabel("Year")
plt.ylabel("Opioid Deaths per 100,000 People")

custom_lines = [
    plt.Line2D([0], [0], color=sns.color_palette()[0], lw=2, label="Florida"),
    plt.Line2D([0], [0], color=sns.color_palette()[1], lw=2, label="Controls"),
    plt.Line2D([0], [0], color="black", linestyle="--", lw=1, label="Policy Change"),
]
ax.legend(handles=custom_lines, loc="upper left")

plt.savefig("30_results/fl_deaths_did_plot.png", dpi=300, bbox_inches="tight")
