import pymysql
import pandas as pd
from config import db

connection = pymysql.connect(host=db.host, user=db.user, password=db.password,database=db.database)

# customer_sql = 'select customer_id,customer_name from t_customer'
# device_sql = 'select device_no,device_name,customer_id,branch_factory from t_device_wide'
# event_sql = 'select customer_id,device_detail_type,device_no,full_cycle_times,free_times,work_times,p_date from t_customer'
#
# df_customer = pd.read_sql_query(customer_sql, connection)
# df_device = pd.read_sql_query(device_sql, connection)
# df_event = pd.read_sql_query(device_sql, connection)
#
# connection.close()
batch_size = 1000
offset = 0
dataframes = []

try:
    while True:
        offset += batch_size
        sql = f"SELECT * FROM t_customer LIMIT {batch_size} OFFSET {offset}"
        df = pd.read_sql_query(sql, connection)

        if df.empty:
            break

        dataframes.append(df)
finally:
    connection.close()

# 合并所有数据帧为一个大的数据帧
all_data = pd.concat(dataframes, ignore_index=True)

