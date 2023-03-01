import configparser
from datetime import datetime
from raw import raw_treatment
from staging import stg_treatment
from sandbox import   months_active_canceled\
                    , months_confirmed_books\
                    , months_since_first_subscription\
                    , months_since_last_status_change

start_execution = datetime.now()
config = configparser.ConfigParser()
config.read('files/config/config.ini')

print("\n===================\nRAW LAYER TREATMENT\n===================")

# -- BOOKING --#
raw_treatment.treat_booking_csv(  config['raw']['path_in']
                                , config['raw']['book_file']
                                , config['raw']['path_out']
                                , config['raw']['book_file']
                                )

# -- SUBSCRIPTION --#
raw_treatment.treat_subscription_csv( config['raw']['path_in']
                                    , config['raw']['sub_file']
                                    , config['raw']['path_out']
                                    , config['raw']['sub_file']
                                    )


print("\n=======================\nSTAGING LAYER TREATMENT\n=======================")

# -- BOOKING --#
stg_treatment.treat_stg_booking(  config['staging']['path_in_out']
                                , config['staging']['book_file']
                                , config['staging']['first_sub_file']
                                , config['staging']['last_book_file']
                                )



print("\n=======================\nSANDBOX LAYER TREATMENT\n=======================")

months_since_first_subscription\
    .generate_months_passed_since_first_sub(
                                              config['sandbox']['path_in']
                                            , config['sandbox']['path_out']
                                            , config['sandbox']['sub_file']
                                            , config['sandbox']['last_book_file_in']
                                            , config['sandbox']['first_sub_file_in']
                                            , config['sandbox']['months_first_sub_file']
                                        )

months_active_canceled\
    .generate_months_per_status(
                                  config['sandbox']['path_in']
                                , config['sandbox']['path_out']
                                , config['sandbox']['sub_file']
                                , config['sandbox']['months_per_status_file']
                            )

months_since_last_status_change\
    .generate_months_status_change(
                                  config['sandbox']['path_in']
                                , config['sandbox']['path_out']
                                , config['sandbox']['sub_file']
                                , config['sandbox']['months_status_change_file']
                            )

months_confirmed_books\
    .generate_months_confirmed_books(
                                  config['sandbox']['path_in']
                                , config['sandbox']['path_out']
                                , config['sandbox']['book_file']
                                , config['sandbox']['booking_confirmed_file']
                              )


print("============================")
print(f"Program executed in {datetime.now() - start_execution}")
print("============================")
