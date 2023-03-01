
def stage_layer_manipulation():

    #----------------------#
    #--------BOOKING-------#
    #----------------------#

    # Getting the first and last subscription date per each id/month
    columns = ['subscriber_id','booking_status','booking_date','booking_year_month','month','date']
    stg_booking_df = pd.read_csv("files/stg/booking.csv")[columns]

    stg_booking_df_filter = stg_booking_df.query("booking_status not in ('Canceled','')")  # option 2 -> df_duplicates = df[df["is_duplicate"] == True]
    first_subscription_df = stg_booking_df_filter.groupby(['subscriber_id']).agg(first_subscription_date=('date', 'min')) #  {'date': ['min']})
    first_subscription_df.to_csv("files/stg/first_subscription_per_id_month.csv")


    # Do I need to use filter in that case as well because the subscription need to be effective only if the status is not canceled?
    stg_booking_df_last = stg_booking_df.groupby(['subscriber_id','booking_year_month']).agg(last_booking_date_per_month=('date', 'max'))
    stg_booking_df_last.to_csv("files/stg/last_booking_per_id_month.csv")



    #----------------------#
    #-----SUBSCRIPTION-----#
    #----------------------#

    stg_subscription_df = pd.read_csv("files/stg/subscription.csv", parse_dates=['sub_date'])
    stg_subscription_df['sub_date'] = stg_subscription_df['sub_year_month']+"-01"


    #--------------------------------------------------------------------#
    #- How many months has passed since their first subscription month? -#
    #--------------------------------------------------------------------#

    # According to the booking table
    months_since_first_sub_df = stg_booking_df_last.merge(first_subscription_df, on='subscriber_id', how='left')
    months_since_first_sub_df['formated_first_date'] = pd.to_datetime(months_since_first_sub_df['first_subscription_date'    ],format='%Y-%m-%d')
    months_since_first_sub_df['formated_last_date' ] = pd.to_datetime(months_since_first_sub_df['last_booking_date_per_month'],format='%Y-%m-%d')
    months_since_first_sub_df['booking_year_month'] = months_since_first_sub_df.apply(lambda x: str(x['last_booking_date_per_month'])[:7], axis=1)
    months_since_first_sub_df['months_since_first_sub'] = (((months_since_first_sub_df['formated_last_date'] - months_since_first_sub_df['formated_first_date'])/np.timedelta64(1,'M')).fillna(0)).astype(int)
    months_since_first_sub_df.drop(labels=['formated_first_date','formated_last_date','last_booking_date_per_month','first_subscription_date'], axis=1, inplace=True)    
    months_since_first_sub_df.to_csv("files/sandbox/booking_months_since_first_sub.csv")


    # According to the subscription table
    # stg_subscription_renamed_df = stg_subscription_df
    stg_subscription_df.rename(columns={'sub_id':'subscriber_id'},inplace=True)
    months_since_first_sub_df = stg_subscription_df.merge(first_subscription_df, on='subscriber_id',how='left')
    months_since_first_sub_df['formated_sub_date'  ] = pd.to_datetime(months_since_first_sub_df['sub_date'],format='%Y-%m-%d')
    months_since_first_sub_df['formated_first_date'] = pd.to_datetime(months_since_first_sub_df['first_subscription_date'],format='%Y-%m-%d')
    months_since_first_sub_df['months_since_first_sub'] = (((months_since_first_sub_df['formated_sub_date'] - months_since_first_sub_df['formated_first_date'])/np.timedelta64(1,'M')).fillna(0)).round(decimals = 0).astype(int)
    months_since_first_sub_df = months_since_first_sub_df[['subscriber_id','sub_year_month','months_since_first_sub']]
    months_since_first_sub_df.to_csv("files/sandbox/subscribing_months_since_first_sub.csv")
    months_since_first_sub_df.sort_values(by = ['subscriber_id', 'sub_year_month'], ascending = [True, True],inplace=True)


    #------------------------------------------------------------#
    #- How many months they were an active/canceled subscriber? -#
    #------------------------------------------------------------#
    
    count_status_df = stg_subscription_df[['subscriber_id','status','sub_year_month']]

    for index, row in count_status_df.iterrows():
        if row['status'].lower().strip() == 'active':
            count_status_df.at[index,'is_active'] = 1
            count_status_df.at[index,'is_canceled'] = 0
        else:
            count_status_df.at[index,'is_active'] = 0
            count_status_df.at[index,'is_canceled'] = 1

    stg_subscription_df.sort_values(by = ['subscriber_id', 'sub_year_month'], ascending = [True, True],inplace=True)
    count_status_df['monhts_active'] = count_status_df['is_active'].cumsum().astype(int)
    count_status_df['monhts_canceled'] = count_status_df['is_canceled'].cumsum().astype(int)

    count_status_df = count_status_df[['subscriber_id', 'sub_year_month', 'monhts_active', 'monhts_canceled']]
    count_status_df.to_csv("files/sandbox/months_active_canceled.csv")


    #---------------------------------------------------------------------------#
    #- How many months has passed since their last subscription status change? -#
    #---------------------------------------------------------------------------#



    months_since_last_status_change_df = stg_subscription_df[['subscriber_id','status','sub_year_month','sub_date']]
    months_since_last_status_change_df['previous_status'] = months_since_last_status_change_df.groupby(['subscriber_id'])['status'].shift(1).fillna('')
    months_since_last_status_change_df['previous_sub_date'] = months_since_last_status_change_df.groupby(['subscriber_id'])['sub_date'].shift(1).fillna('')
    
    #option 1:
    # for index, row in months_since_last_status_change_df.iterrows():        
    #     if row['previous_status'] != '':
    #         if row['status'] != row['previous_status']:
    #             months_since_last_status_change_df.at[index,'status_change'] = 1
    # test = months_since_last_status_change_df.query("status_change == 1")

    #option2: GETTING THE FIRTS STATUS CHANGE DATE PER ID
    test = months_since_last_status_change_df.query("status != previous_status and previous_status != ''")
    test = test.groupby(['subscriber_id']).agg(first_status_change=('sub_date', 'min'))
    dict = test.to_dict('dict')

    for index, row in months_since_last_status_change_df.iterrows():
        for values in dict.values():
            for i in values:
                if row['subscriber_id'] == i and row['sub_date'] == values[i]:
                    months_since_last_status_change_df.at[index,'first_status_change'] = True
                    months_since_last_status_change_df.at[index,'idc_status_change'] = 1

            if row['status'] != row['previous_status'] and row['previous_status'] != '':
                months_since_last_status_change_df.at[index,'idc_status_change'] = 1
            else:
                months_since_last_status_change_df.at[index,'idc_status_change'] = 0

    months_since_last_status_change_df['idc_status_change'] = months_since_last_status_change_df['idc_status_change'].astype(int)


    months_since_last_status_change_df.sort_values(by = ['subscriber_id', 'sub_year_month'], ascending = [True, True],inplace=True)
    months_since_last_status_change_df['formated_sub_date'] = pd.to_datetime(months_since_last_status_change_df['sub_date'],format='%Y-%m-%d')
    months_since_last_status_change_df['formated_previous_sub_date'] = pd.to_datetime(months_since_last_status_change_df['previous_sub_date'],format='%Y-%m-%d')
    months_since_last_status_change_df['previous_idc_status_change'] = months_since_last_status_change_df.groupby(['subscriber_id'])['idc_status_change'].shift(1).fillna(-1).astype(int)


    for index, row in months_since_last_status_change_df.iterrows():
        if row['first_status_change'] == True:
            months_since_last_status_change_df.at[index,'months_since_last_change'] = 0
            # last_change = row['formated_sub_date']
            # diff_months = ((row['formated_sub_date'] - last_change)/np.timedelta64(1,'M'))
            # months_since_last_status_change_df.at[index,'months_since_last_change'] = diff_months
            # months_since_last_status_change_df.at[index,'last_change'] = last_change

        elif (row['idc_status_change'] == 1 or row['idc_status_change'] == 0) and row['previous_idc_status_change'] == 1:
            last_change = row['formated_previous_sub_date']
            diff_months = ((row['formated_sub_date'] - last_change)/np.timedelta64(1,'M'))
            months_since_last_status_change_df.at[index,'months_since_last_change'] = diff_months
            months_since_last_status_change_df.at[index,'last_change'] = last_change


    for index, row in months_since_last_status_change_df.iterrows():
        if (row['idc_status_change'] == 1 or row['idc_status_change'] == 0) and (row['previous_idc_status_change']) == 0:
            months_since_last_status_change_df.loc[index,'last_change'] = months_since_last_status_change_df.loc[index-1,'last_change']


    for index, row in months_since_last_status_change_df.iterrows():
        for values in dict.values():
            for i in values:
                formated_date = datetime.strptime(values[i], "%Y-%m-%d")                    
                if row['subscriber_id'] == i and row['formated_sub_date'] <= formated_date:
                    months_since_last_status_change_df.at[index,'last_change'] = row['formated_sub_date']            

    last_change = months_since_last_status_change_df['last_change']
    formated_date = months_since_last_status_change_df['formated_sub_date']    
    months_since_last_status_change_df['months_since_last_change'] = (((formated_date-last_change)/np.timedelta64(1,'M')).fillna(0)).round(decimals = 0).astype(int)









    # for index, row in months_since_last_status_change_df.iterrows():
    #     if (row['idc_status_change'] == 1 or row['idc_status_change'] == 0) and (row['previous_idc_status_change']) == 0:
    #         diff_months = ((row['formated_sub_date'] - row['last_change'])/np.timedelta64(1,'M'))
    #         months_since_last_status_change_df.at[index,'months_since_last_change'] = diff_months

    #     # elif  and row['previous_idc_status_change'] == 1:
    #     #     last_change = row['formated_previous_sub_date']
    #     #     diff_months = ((row['formated_sub_date'] - last_change)/np.timedelta64(1,'M'))
    #     #     months_since_last_status_change_df.at[index,'months_since_last_change'] = diff_months


    months_since_last_status_change_df = months_since_last_status_change_df[['subscriber_id','sub_year_month', 'status','months_since_last_change']]
    display(months_since_last_status_change_df)

    months_since_last_status_change_df.to_csv("files/sandbox/months_since_last_status_change.csv")
    
    
    # months_since_last_status_change_df = months_since_last_status_change_df['first_status_change'].astype(bool)
    # print(months_since_last_status_change_df.dtypes)


    # for index, row in months_since_last_status_change_df.iterrows():
    #     # last_change = row['formated_sub_date']
    #     if row['first_status_change'] == True:
    #         last_change = row['formated_sub_date']
    #         # print("ENTREI")
    #         diff_months = ((row['formated_sub_date'] - last_change)/np.timedelta64(1,'M'))
    #         # print(diff_months)
    #         months_since_last_status_change_df.at[index,'months_since_last_change'] = diff_months
    #     else:
    #         if row['status']==row['previous_status']:
    #             _last_change = last_change
    #             diff_months = ((row['formated_sub_date'] - _last_change)/np.timedelta64(1,'M'))                
    #             months_since_last_status_change_df.at[index,'months_since_last_change'] = diff_months

    # display(months_since_last_status_change_df)








    #-------------------------------------------------#
    #- How many confirmed bookings did they receive? -#
    #-------------------------------------------------#

    count_bookings_df = stg_booking_df
    
    for index, row in count_bookings_df.iterrows():
        if row['booking_status'].lower().strip() == 'confirmed':
            count_bookings_df.at[index,'is_confirmed'] = 1
        else:
            count_bookings_df.at[index,'is_confirmed'] = 0

    count_bookings_df = count_bookings_df.groupby(['subscriber_id','booking_year_month']).agg(sum_confirmed=('is_confirmed', 'sum'))
    count_bookings_df['bookings_confirmed_count'] = count_bookings_df['sum_confirmed'].cumsum()
    count_bookings_df.to_csv("files/stg/bookings_confirmed.csv")

