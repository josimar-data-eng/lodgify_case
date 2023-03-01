import numpy as np
import pandas as pd


def fill_na(df):
    columns_list = df.columns
    for i in columns_list:
        if df[i].dtype == 'object':
            df[i].fillna('',inplace=True)
        elif df[i].dtype in ('float64','int64'):
            df[i].fillna(-1,inplace=True)
    return df


def positional_function(operation,df,in_colum,out_colum,column_grouped):

    if operation == 'lag':
        df[out_colum] = df.groupby\
                            ([column_grouped])\
                            [in_colum]\
                            .shift(1)
    else:
        if operation == 'lead':
            df[out_colum] = df.groupby\
                                ([column_grouped])\
                                [in_colum]\
                                .shift(-1)

    if df[out_colum].dtype not in ('float64','int64'): 
        df[out_colum] = df[out_colum].fillna('')  
    else:
        df[out_colum] = df[out_colum].fillna(-1).astype(int)

    return df


def date_diff_months(df,start_date,end_date,round,out_column,has_round):
    if has_round:
        df[out_column] = ((
                            ((df[end_date] - df[start_date]))/
                            np.timedelta64(1,'M')
                        ).fillna(0)
                        ).round(decimals = round)\
                        .astype(int)
    else:
        df[out_column] = ((
                            ((df[end_date] - df[start_date]))/
                            np.timedelta64(1,'M')
                        ).fillna(0)
                        ).astype(int)
    return df


def parse_to_datetime(df,source_format,column):
    if source_format == 'object':
        df[column] = pd.to_datetime( df[column]
                                    ,format='%Y-%m-%d'
                                    )
    return df


def create_primary_key(df):
    df = df.reset_index()
    df = df.rename(columns={"index":"id"})
    df["id"] = df.index + 0
    return df


def groupby(df,columns_to_group,column_to_apply_agg,agg_function,out_column_name):    
    df = df\
        .groupby(
                columns_to_group
                )\
        .agg(
            agg_column =(column_to_apply_agg, agg_function)
            )
    df.rename(columns={"agg_column":out_column_name},inplace=True)
    return df
