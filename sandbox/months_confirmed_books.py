import pandas as pd
import operations as ops
from IPython.display import display



    #-------------------------------------------------#
    #- How many confirmed bookings did they receive? -#
    #-------------------------------------------------#

def test():
    columns = ['subscriber_id','booking_status','booking_date','booking_year_month','month','date']
    stg_booking_df = pd.read_csv("files/stg/booking.csv")[columns]

    count_bookings_df = stg_booking_df
    
    for index, row in count_bookings_df.iterrows():
        if row['booking_status'].lower().strip() == 'confirmed':
            count_bookings_df.at[index,'is_confirmed'] = 1
        else:
            count_bookings_df.at[index,'is_confirmed'] = 0

    count_bookings_df = count_bookings_df.groupby(['subscriber_id','booking_year_month']).agg(sum_confirmed=('is_confirmed', 'sum'))
    count_bookings_df['bookings_confirmed_count'] = count_bookings_df['sum_confirmed'].cumsum()
    count_bookings_df.to_csv("files/stg/bookings_confirmed.csv")

