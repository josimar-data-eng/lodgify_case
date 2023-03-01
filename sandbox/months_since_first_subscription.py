import pandas as pd
import operations as ops
from IPython.display import display


def get_months_passed_since_first_sub(
    path_in, path_out, file_in, last_book_file_in, first_sub_file_in, file_out
):
    # ---------------------#
    # - Reading stg files -# To refactor
    # ---------------------#

    stg_subscription_df = pd.read_csv(
        path_in + file_in, parse_dates=["sub_date"]
    )
    last_booking_df = pd.read_csv(
        path_in + last_book_file_in, parse_dates=["last_booking_date"]
    )
    first_subscription_df = pd.read_csv(
        path_in + first_sub_file_in, parse_dates=["first_subscription_date"]
    )

    # --------------------------------------------------------------------#
    # - How many months has passed since their first subscription month? -#
    # --------------------------------------------------------------------#

    # ----- According to the booking table -----#
    months_since_first_sub_df = last_booking_df.merge(
        first_subscription_df, on="subscriber_id", how="left"
    )

    ops.date_diff_months(
        months_since_first_sub_df,
        "first_subscription_date",
        "last_booking_date",
        0,
        "months_since_first_sub",
        False,
    )

    months_since_first_sub_df.drop(
        labels=["last_booking_date", "first_subscription_date"], axis=1, inplace=True
    )
    new_file_out = (
        file_out.replace((file_out[len(file_out) - 4 :]), "")
        + "_approach1"
        + file_out[len(file_out) - 4 :]
    )
    months_since_first_sub_df.to_csv(path_out + new_file_out)
    print(
        f"File {new_file_out} loaded at {path_out} directory with {len(months_since_first_sub_df.index)} rows."
    )

    # ----- According to the subscription table -----#
    stg_subscription_df.rename(columns={"sub_id": "subscriber_id"}, inplace=True)

    months_since_first_sub_df = stg_subscription_df.merge(
        first_subscription_df, on="subscriber_id", how="left"
    )

    ops.date_diff_months(
        months_since_first_sub_df,
        "first_subscription_date",
        "sub_date",
        0,
        "qty_months_since_first_sub",
        True,
    )

    months_since_first_sub_df = months_since_first_sub_df[
        ["subscriber_id", "sub_year_month", "qty_months_since_first_sub"]
    ]
    new_file_out = (
        file_out.replace((file_out[len(file_out) - 4 :]), "")
        + "_approach2"
        + file_out[len(file_out) - 4 :]
    )
    months_since_first_sub_df.to_csv(path_out + new_file_out)
    print(
        f"File {new_file_out} loaded at {path_out} directory with {len(months_since_first_sub_df.index)} rows."
    )
