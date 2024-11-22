# necessary packages

import pandas as pd
import numpy as np
import seaborn.objects as so
import seaborn as sns
import matplotlib.pyplot as plt

pd.set_option("mode.copy_on_write", True)

# datasets

fl_drugs_did = pd.read_parquet("20_intermediate_files/fl_controls_prescrip.parquet")

fl_drugs_did["MMEPC"] = fl_drugs_did["MME"] / fl_drugs_did["Population"]

fl_drugs_pp = fl_drugs_did[fl_drugs_did["STATE"] == "FL"]
fl_drugs_pp["post"] = np.where(fl_drugs_pp["YEAR"] > 2009, 1, 0)

fl_drugs_did["DID"] = np.where(
    (fl_drugs_did["STATE"] == "FL") & (fl_drugs_did["YEAR"] < 2010),
    1,
    np.where(
        (fl_drugs_did["STATE"] != "FL") & (fl_drugs_did["YEAR"] < 2010),
        2,
        np.where(
            (fl_drugs_did["STATE"] == "FL") & (fl_drugs_did["YEAR"] > 2009),
            3,
            np.where(
                (fl_drugs_did["STATE"] != "FL") & (fl_drugs_did["YEAR"] > 2009),
                4,
                np.nan,
            ),
        ),
    ),
)

wa_drugs_did = pd.read_parquet("20_intermediate_files/wa_controls_prescrip.parquet")

wa_drugs_did["MMEPC"] = wa_drugs_did["CALC_MME"] / wa_drugs_did["Population"]

wa_drugs_pp = wa_drugs_did[wa_drugs_did["STATE"] == "WA"]
wa_drugs_pp["post"] = np.where(wa_drugs_pp["YEAR"] > 2011, 1, 0)

wa_drugs_did["DID"] = np.where(
    (wa_drugs_did["STATE"] == "WA") & (wa_drugs_did["YEAR"] < 2012),
    1,
    np.where(
        (wa_drugs_did["STATE"] != "WA") & (wa_drugs_did["YEAR"] < 2012),
        2,
        np.where(
            (wa_drugs_did["STATE"] == "WA") & (wa_drugs_did["YEAR"] > 2011),
            3,
            np.where(
                (wa_drugs_did["STATE"] != "WA") & (wa_drugs_did["YEAR"] > 2011),
                4,
                np.nan,
            ),
        ),
    ),
)

# Washington Drugs Pre-Post Plot

wa_drugs_pp_plot = sns.lmplot(
    data=wa_drugs_pp,
    x="YEAR",
    y="MMEPC",
    hue="post",
    ci=95,
    scatter=False,
    line_kws={"lw": 2},
    legend=False,
    aspect=1.5,
)

ax = wa_drugs_pp_plot.ax

ax.axvline(x=2012, color="black", linestyle="--", linewidth=1, label="Policy Change")

ax.set_title("Washington Opioid (MME) Shipments Per Capita (Pre vs. Post Policy)")
ax.set_xlabel("Year")
ax.set_ylabel("Opioid (MME) Shipments Per Capita")

custom_lines = [
    plt.Line2D([0], [0], color=sns.color_palette()[0], lw=2, label="Pre-Policy"),
    plt.Line2D([0], [0], color=sns.color_palette()[1], lw=2, label="Post-Policy"),
    plt.Line2D([0], [0], color="black", linestyle="--", lw=1, label="Policy Change"),
]
ax.legend(handles=custom_lines, loc="upper left")

plt.savefig("30_results/wa_drugs_pp_plot.png", dpi=300, bbox_inches="tight")

# Florida Drugs Pre-Post Plot

fl_drugs_pp_plot = sns.lmplot(
    data=fl_drugs_pp,
    x="YEAR",
    y="MMEPC",
    hue="post",
    ci=95,
    scatter=False,
    line_kws={"lw": 2},
    legend=False,
    aspect=1.5,
)

ax = fl_drugs_pp_plot.ax

ax.axvline(x=2010, color="black", linestyle="--", linewidth=1, label="Policy Change")

ax.set_title("Florida Opioid (MME) Shipments Per Capita (Pre vs. Post Policy)")
ax.set_xlabel("Year")
ax.set_ylabel("Opioid (MME) Shipments Per Capita")

custom_lines = [
    plt.Line2D([0], [0], color=sns.color_palette()[0], lw=2, label="Pre-Policy"),
    plt.Line2D([0], [0], color=sns.color_palette()[1], lw=2, label="Post-Policy"),
    plt.Line2D([0], [0], color="black", linestyle="--", lw=1, label="Policy Change"),
]
ax.legend(handles=custom_lines, loc="upper left")

plt.savefig("30_results/fl_drugs_pp_plot.png", dpi=300, bbox_inches="tight")

# Washington Drugs Diff-in-Diff Plot

wa_drugs_did_plot = sns.lmplot(
    data=wa_drugs_did,
    x="YEAR",
    y="MMEPC",
    hue="DID",
    palette={1: "blue", 2: "orange", 3: "blue", 4: "orange"},
    ci=95,
    scatter=False,
    line_kws={"lw": 2},
    legend=False,
    aspect=1.5,
)

ax = wa_drugs_did_plot.ax

plt.axvline(x=2012, color="black", linestyle="--", linewidth=1, label="Policy Change")

plt.title("Opioid (MME) Shipments Per Capita (Pre vs. Post Policy)")
plt.xlabel("Year")
plt.ylabel("Opioid (MME) Shipments Per Capita")

custom_lines = [
    plt.Line2D([0], [0], color=sns.color_palette()[0], lw=2, label="Washington"),
    plt.Line2D([0], [0], color=sns.color_palette()[1], lw=2, label="Controls"),
    plt.Line2D([0], [0], color="black", linestyle="--", lw=1, label="Policy Change"),
]
ax.legend(handles=custom_lines, loc="upper left")

plt.savefig("30_results/wa_drugs_did_plot.png", dpi=300, bbox_inches="tight")

# Florida Drugs Diff-in-Diff Plot

fl_drugs_did_plot = sns.lmplot(
    data=fl_drugs_did,
    x="YEAR",
    y="MMEPC",
    hue="DID",
    palette={1: "blue", 2: "orange", 3: "blue", 4: "orange"},
    ci=95,
    scatter=False,
    line_kws={"lw": 2},
    legend=False,
    aspect=1.5,
)

ax = fl_drugs_did_plot.ax

plt.axvline(x=2010, color="black", linestyle="--", linewidth=1, label="Policy Change")

plt.title("pioid (MME) Shipments Per Capita (Pre vs. Post Policy)")
plt.xlabel("Year")
plt.ylabel("pioid (MME) Shipments Per Capita")

custom_lines = [
    plt.Line2D([0], [0], color=sns.color_palette()[0], lw=2, label="Florida"),
    plt.Line2D([0], [0], color=sns.color_palette()[1], lw=2, label="Controls"),
    plt.Line2D([0], [0], color="black", linestyle="--", lw=1, label="Policy Change"),
]
ax.legend(handles=custom_lines, loc="upper left")

plt.savefig("30_results/fl_drugs_did_plot.png", dpi=300, bbox_inches="tight")
