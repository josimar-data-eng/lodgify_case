import pandas as pd
import datetime as dt
import pandas_utils as utils
from IPython.display import display
from pandas.tseries.offsets import DateOffset


def treat_booking_csv(path_in, file_in, path_out, file_out):
    # Reading csv and getting only the coluns needed and trustable. We can set nan(nulls) to empty using na_filter parameter.
    columns = ["subscriber_id", "booking_status", "booking_date"]
    raw_booking_df = pd.read_csv(path_in + file_in)[columns]

    # filter to make tests
    # raw_booking_df = raw_booking_df.loc[raw_booking_df['subscriber_id'].isin([156,256])]

    # Removing duplicates
    raw_booking_df = raw_booking_df.drop_duplicates()

    # Handling missing data
    utils.fill_na(raw_booking_df)

    # Inserting necessary columns to define first and last bookings according to each month
    for idx, row in raw_booking_df.iterrows():
        date = str(row["booking_date"])[:10]
        month = str(row["booking_date"])[5:7]
        year_month = (str(row["booking_date"]))[:7]
        raw_booking_df.at[idx, "date"] = date
        raw_booking_df.at[idx, "month"] = month
        raw_booking_df.at[idx, "booking_year_month"] = year_month

    # Creating primary key
    raw_booking_df = utils.create_primary_key(raw_booking_df)

    # Creating datetime columns to know the loading time
    raw_booking_df["datetime_load"] = dt.datetime.now()

    raw_booking_df.to_csv(path_out + file_out)

    print(
        f"File {file_out} loaded at {path_out} directory with {len(raw_booking_df.index)} rows."
    )


def treat_subscription_csv(path_in, file_in, path_out, file_out):
    # reading csv and selecting only the necessary and trust columns
    columns = ["sub_id", "status", "dates"]
    raw_subscription_df = pd.read_csv(path_in + file_in)[columns]

    # Deduplicating
    raw_subscription_df.drop_duplicates()

    # Handling missing data
    utils.fill_na(raw_subscription_df)

    # filter to make tests
    # raw_subscription_df = raw_subscription_df.loc[raw_subscription_df['sub_id'].isin([156,256])]

    # Adjusting dates field to be able to parse it to datetime for two purposes:
    #   1 - get status from a previous existing/single month
    #   2 - make sure we can fill the month gaps correctly and
    # but I'm not sure if fill the month gaps is necessary. Can anyone help me with this misunderstanding?

    # ----------------------------------------------------------#
    # getting the status from a previous existing/single month #
    # ----------------------------------------------------------#

    # Creating a formated columns using lambda funcion to apply in axis 1 (columns) the concatenation
    raw_subscription_df["formated_dates"] = raw_subscription_df.apply(
        lambda x: (x["dates"] + "-01"), axis=1
    )
    utils.parse_to_datetime(
        raw_subscription_df,
        raw_subscription_df["formated_dates"].dtype,
        "formated_dates",
    )

    # sorting to create next_date, previous_date, and previous_status columns correctly
    raw_subscription_df.sort_values(
        by=["sub_id", "formated_dates"], ascending=[True, True], inplace=True
    )

    # Lead and Lag Functions to create the months's difference between the records to check the fill gap month needed
    utils.positional_function(
        "lag", raw_subscription_df, "formated_dates", "previous_date", "sub_id"
    )
    utils.positional_function(
        "lead", raw_subscription_df, "formated_dates", "next_date", "sub_id"
    )

    # For loop to flag if the month-year is duplicated, to use it as a criteria to get the status from the previous month
    for idx, row in raw_subscription_df.iterrows():
        if (
            row["formated_dates"] == row["previous_date"]
            or row["formated_dates"] == row["next_date"]
        ):
            raw_subscription_df.at[idx, "is_duplicate"] = True
        else:
            raw_subscription_df.at[idx, "is_duplicate"] = False

    # Remove the month-year duplicates
    raw_subscription_df = raw_subscription_df.drop_duplicates(
        subset=["sub_id", "dates"]
    )

    raw_subscription_df.sort_values(
        by=["sub_id", "formated_dates"], ascending=[True, True], inplace=True
    )

    # Reset the index to make sure the previous month's status will be on previous row.
    raw_subscription_df = raw_subscription_df.reset_index()

    # Filling the status column getting from a previous existing/single month using the is_duplicate field created above as criteria
    for idx, row in raw_subscription_df.iterrows():
        if row["is_duplicate"] == True:
            raw_subscription_df.loc[idx, "status"] = raw_subscription_df.loc[
                idx - 1, "status"
            ]

    # ------------------------#
    # Filling the month gaps #
    # ------------------------#

    # # Adding new rows to fill the month gaps and assigning the status from a previous existing/single month
    #       For this step, I'm not sure if that's the case asked, or if it was only my understand.
    #       btw, if it was only my understand just comment the following two steps , otherwise uncomment

    # Step 1
    # diff dates to get month difference to be able to fill the gap months correctly
    utils.date_diff_months(
        raw_subscription_df, "formated_dates", "next_date", 0, "diff_months", True
    )

    # Step 2
    # # Iterating through the dataframe to add the missing months between one date and another.
    for index, row in raw_subscription_df.iterrows():
        if (row["diff_months"]) > 1:
            _index = row["diff_months"]
            for i in range(1, _index):
                new_row = pd.DataFrame(
                    {
                        "sub_id": [row["sub_id"]],
                        "status": [row["status"]],
                        "formated_dates": [
                            row["formated_dates"] + pd.DateOffset(months=1)
                        ],
                    }
                )
                row["formated_dates"] += pd.DateOffset(months=1)
                raw_subscription_df = pd.concat([raw_subscription_df, new_row])

    raw_subscription_df.sort_values(
        by=["sub_id", "formated_dates"], ascending=[True, True], inplace=True
    )

    # Creating sub_year_month column
    raw_subscription_df["formated_date"] = raw_subscription_df.apply(
        lambda x: str(x["formated_dates"])[:7], axis=1
    )

    raw_subscription_df.rename(
        columns={"formated_dates": "sub_date", "formated_date": "sub_year_month"},
        inplace=True,
    )

    # Creating primary key and datetime load column
    raw_subscription_df = utils.create_primary_key(raw_subscription_df)
    raw_subscription_df["datetime_load"] = dt.datetime.now()

    # Filtering only necessary columns
    raw_subscription_df = raw_subscription_df[
        ["id", "sub_id", "status", "sub_year_month", "sub_date", "datetime_load"]
    ]
    raw_subscription_df.to_csv(path_out + file_out)

    print(
        f"File {file_out} loaded at {path_out} directory with {len(raw_subscription_df.index)} rows.\n"
    )
