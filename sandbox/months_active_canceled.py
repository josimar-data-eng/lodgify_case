import pandas as pd
from IPython.display import display


def generate_months_per_status(path_in,path_out,file_in,file_out):

    #------------------------------------------------------------#
    #- How many months they were an active/canceled subscriber? -#
    #------------------------------------------------------------#


    #-- Reading stg file --#
    stg_subscription_df = pd.read_csv(path_in+file_in)

    #Selecting only necessary columns
    count_status_df = stg_subscription_df\
                                    .filter(
                                            ['sub_id','status','sub_year_month']
                                            )

    # iterating throug dataframe to create a numeric column that will be used to sum over operation
    for index, row in count_status_df.iterrows():
        if row['status'].lower().strip() == 'active':
            count_status_df.at[index,'is_active'] = 1
            count_status_df.at[index,'is_canceled'] = 0
        else:
            count_status_df.at[index,'is_active'] = 0
            count_status_df.at[index,'is_canceled'] = 1


    count_status_df.sort_values(by =  ['sub_id', 'sub_year_month']
                                    , ascending = [True, True]
                                    , inplace=True
                                )
    
    count_status_df['qtt_monhts_active'] = count_status_df\
                                                    .groupby(
                                                            ['sub_id']
                                                            )\
                                                    ['is_active'].cumsum().astype(int)

    count_status_df['qtt_monhts_canceled'] = count_status_df\
                                                    .groupby(
                                                            ['sub_id']
                                                            )\
                                                    ['is_canceled'].cumsum().astype(int)

    count_status_df = count_status_df\
                                .filter(
                                        ['sub_id','sub_year_month','qtt_monhts_active','qtt_monhts_canceled']
                                        )
    count_status_df.to_csv(path_out+file_out)
    print(f"File {file_out} loaded at {path_out} directory with {len(count_status_df.index)} rows.")        

    # display(stg_subscription_df)
