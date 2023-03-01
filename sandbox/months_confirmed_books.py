import pandas as pd
import pandas_utils as utils
from IPython.display import display


# -------------------------------------------------#
# - How many confirmed bookings did they receive? -#
# -------------------------------------------------#


def get_months_confirmed_books(path_in, path_out, file_in, file_out):
    # columns = ['subscriber_id','booking_status','booking_date','booking_year_month','month','date']
    stg_booking_df = pd.read_csv(path_in + file_in)  # [columns]

    stg_booking_df = stg_booking_df.filter(
        [
            "subscriber_id",
            "booking_status",
            "booking_date",
            "booking_year_month",
            "month",
            "date",
        ]
    )

    for index, row in stg_booking_df.iterrows():
        if row["booking_status"].lower().strip() == "confirmed":
            stg_booking_df.at[index, "idc_book_confirmed"] = 1
        else:
            stg_booking_df.at[index, "idc_book_confirmed"] = 0

    months_confirmed_books = utils.groupby(
        stg_booking_df,
        ["subscriber_id", "booking_year_month"],
        "idc_book_confirmed",
        "sum",
        "sum_confirmed",
    ).astype(int)

    # Sum Over
    months_confirmed_books["qty_confirmed_bookings"] = (
        months_confirmed_books.groupby(["subscriber_id", "booking_year_month"])[
            "sum_confirmed"
        ]
        .cumsum()
        .astype(int)
    )

    # display(months_confirmed_books)
    months_confirmed_books.to_csv(path_out + file_out)
    print(
        f"File {file_out} loaded at {path_out} directory with {len(months_confirmed_books.index)} rows.\n\n"
    )
