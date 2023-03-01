import pandas as pd
import operations as ops
from IPython.display import display
from pandas.tseries.offsets import DateOffset


def get_months_status_change(path_in, path_out, file_in, file_out):
    # ---------------------------------------------------------------------------#
    # - How many months has passed since their last subscription status change? -#
    # ---------------------------------------------------------------------------#

    stg_subscription_df = pd.read_csv(path_in + file_in, parse_dates=["sub_date"])
    # stg_subscription_df = pd.read_csv("staging/files/subscription.csv", parse_dates=['sub_date'])

    months_status_change_df = stg_subscription_df.filter(
        ["sub_id", "status", "sub_year_month", "sub_date"]
    )
    months_status_change_df.sort_values(
        by=["sub_id", "sub_year_month"], ascending=[True, True], inplace=True
    )

    ops.positional_function(
        "lag", months_status_change_df, "status", "previous_status", "sub_id"
    )
    ops.positional_function(
        "lag", months_status_change_df, "sub_date", "previous_sub_date", "sub_id"
    )

    # option 1: Marking if it was status change
    for index, row in months_status_change_df.iterrows():
        if row["previous_status"] != "":
            if row["status"] != row["previous_status"]:
                months_status_change_df.at[index, "idc_status_change"] = 1
            else:
                months_status_change_df.at[index, "idc_status_change"] = 0
        else:
            months_status_change_df.at[index, "idc_status_change"] = 0
    months_status_change_df["idc_status_change"] = months_status_change_df[
        "idc_status_change"
    ].astype(int)

    only_status_change_df = months_status_change_df.query("idc_status_change == 1")

    first_status_change_df = ops.groupby(
        only_status_change_df, ["sub_id"], "sub_date", "min", "first_status_change"
    )
    first_status_change_dict = first_status_change_df.to_dict("dict")

    for index, row in months_status_change_df.iterrows():
        for values in first_status_change_dict.values():
            for i in values:
                if row["sub_id"] == i and row["sub_date"] == values[i]:
                    months_status_change_df.at[index, "first_status_change"] = True

    # To refactor, because this specific value can't be filled
    ops.positional_function(
        "lag",
        months_status_change_df,
        "idc_status_change",
        "previous_idc_status_change",
        "sub_id",
    )

    for index, row in months_status_change_df.iterrows():
        if row["first_status_change"] == True:
            last_change = row["sub_date"]
            months_status_change_df.at[index, "last_change"] = last_change

        else:  # If had change in the previous row
            if row["previous_idc_status_change"] == 1:
                last_change = row["previous_sub_date"]
                months_status_change_df.at[index, "last_change"] = last_change

    for index, row in months_status_change_df.iterrows():
        if (row["previous_idc_status_change"]) == 0 and row[
            "first_status_change"
        ] != True:
            months_status_change_df.loc[
                index, "last_change"
            ] = months_status_change_df.loc[index - 1, "last_change"]

    for index, row in months_status_change_df.iterrows():
        for values in first_status_change_dict.values():
            for i in values:
                if row["sub_id"] == i and row["sub_date"] < values[i]:
                    months_status_change_df.at[index, "last_change"] = row["sub_date"]

    ops.date_diff_months(
        months_status_change_df, "last_change", "sub_date", 0, "qty_months_since_last_change", True
    )

    months_status_change_df = months_status_change_df.filter(
        ["sub_id", "status", "sub_year_month", "qty_months_since_last_change"]
    )

    # display(months_status_change_df)
    months_status_change_df.to_csv(path_out + file_out)
    print(
        f"File {file_out} loaded at {path_out} directory with {len(months_status_change_df.index)} rows."
    )
