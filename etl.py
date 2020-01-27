import os
import pandas as pd
import re
import pyodbc
from datetime import datetime as dt
import time
import numpy as np
# from pymongo import MongoClient
# from pymongo.bulk import BulkWriteError


def remove_non_numbers(cnpj):
    pattern = "\D"
    return re.sub(pattern, '', cnpj)

def create_query(values):
    query = """
            insert into asset_investmentfunddailyprices (
            data_provider_id, cnpj, date, timestamp, portfolio_value, 
            share_value, net_worth_value, daily_income, daily_withdraw,
            number_shareholders, updated_at) values """

    for i, value in enumerate(values):    
        if i < len(values) - 1:
            query += """({}, '{}', '{}', '{}', {}, {}, {}, {}, {}, {}, '{}'),""".format(*value)
        else:
            query += """({}, '{}', '{}', '{}', {}, {}, {}, {}, {}, {}, '{}');""".format(*value)
    
    return query

def insert_data(df, cur):
    values = []
    for _, row in df.iterrows():
        line = [1, row['CNPJ_FUNDO'], row['DT_COMPTC'],
                row['DT_COMPTC'].timestamp(), row['VL_TOTAL'],
                row['VL_QUOTA'],row['VL_PATRIM_LIQ'], row['CAPTC_DIA'],
                row['RESG_DIA'], row['NR_COTST'], dt.now()]
        values.append(line)
        if len(values) == 10000:
            query = create_query(values)
            cursor.execute(query)
            conn.commit()
            values = []

    query = create_query(values)
    cursor.execute(query)
    conn.commit()


# def insert_data_mongo(df, client):
#     data = df.to_dict('records')
#     db = client.api_web
#     try:
#         db.asset_investmentfunddailyprices.insert_many(data)
#     except BulkWriteError as bwe:
#         print(bwe.details)

t0 = time.time()

conn_str = ("DRIVER={Postgres3};"
           "DATABASE=api_web;"
           "UID=api_web;"
           "PWD=shoov3Phezaimahsh7eb2Tii4ohkah8k;"
           "SERVER=localhost;"
           "PORT=5432;")


conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# client = MongoClient('mongodb://api_web:shoov3Phezaimahsh7eb2Tii4ohkah8k@localhost:27017/api_web')
# counter = 1

years = sorted(os.listdir('data'))
for folder in years:
    path = 'data/{}'.format(folder)
    print("Inserting year {}".format(folder))
    for i, file in enumerate(os.listdir(path)):
        print("Inserting month {}".format(i + 1))
        file_path = '{}/{}'.format(path, file)
        df = pd.read_csv(file_path, sep=';')
        df['CNPJ_FUNDO'] = df['CNPJ_FUNDO'].apply(remove_non_numbers)
        df['DT_COMPTC'] = pd.to_datetime(df['DT_COMPTC'])
        df = df.replace(r'^\s*$', np.nan, regex=True)
        df = df.fillna('NULL')
        df = df.drop_duplicates()
        # cols = ['cnpj', 'date', 'portfolio_value', 'share_value', 
        #         'net_worth_value', 'daily_income', 'daily_withdraw', 'number_shareholders']
       
        # df.columns = cols
        # df['timestamp'] = df['date'].apply(lambda x: x.timestamp())
        # df['data_provider_id'] = 1
        # df['updated_at'] = dt.now()
        # df['id'] = df.index + counter

        insert_data(df, cursor)
    print("###############################")
        
conn.close()
# client.close()

t1 = time.time()
minutes = (t1 - t0)/60

print("Finished! I took {} minutes".format(minutes))        
