import pandas as pd
import operations as ops
from IPython.display import display


def treat_stg_booking(path_in_out, file_in, first_sub_file, last_book_file):
    # ----------------------#
    # --------BOOKING-------#
    # ----------------------#

    # -------------------------------------------------#
    # Getting the first subscription date per each id #
    # -------------------------------------------------#

    # Should the first subscription be in the booking table? I'm not sure, but I can't get the first
    # subscription from the subscription table because of it's a snapshot with the last day of every month
    # The first_subscription table is created to calculate how many months has passes since the 1o subscription that will be loaded in the sandbox layer.
    columns = [
        "subscriber_id",
        "booking_status",
        "booking_date",
        "booking_year_month",
        "month",
        "date",
    ]
    stg_booking_df = pd.read_csv(path_in_out + file_in)[columns]

    stg_booking_df = stg_booking_df.query("booking_status not in ('Canceled','')")

    # Getting the first date per id and defining it as first_subscription_date
    first_subscription_df = ops.groupby(
        stg_booking_df,
        ["subscriber_id"],
        "booking_date",
        "min",
        "first_subscription_date",
    )
    first_subscription_df.to_csv(path_in_out + first_sub_file)
    print(
        f"File {first_sub_file} loaded at {path_in_out} directory with {len(first_subscription_df.index)} rows."
    )

    # -------------------------------------------#
    # Getting the last booking date per each id #
    # -------------------------------------------#

    # The last_booking table will be used to compare with fist_subscription table to get the months passed since 1o subscription according the bookin table point of view.
    # Do I need to filter the satus? Is the subscription effective only if the status is not canceled?
    last_booking_df = ops.groupby(
        stg_booking_df,
        ["subscriber_id", "booking_year_month"],
        "booking_date",
        "max",
        "last_booking_date",
    )
    last_booking_df.to_csv(path_in_out + last_book_file)
    print(
        f"File {last_book_file} loaded at {path_in_out} directory with {len(last_booking_df.index)} rows.\n"
    )
