import pymysql
import pandas as pd
from config import db

connection = pymysql.connect(host=db.host, user=db.user, password=db.password,database=db.database)

batch_size = 1000
offset = 0
dataframes_customer = []
dataframes_device = []
dataframes_event = []

try:

    sql = f"SELECT customer_id,customer_name FROM t_customer LIMIT {batch_size}"
    df_customer = pd.read_sql_query(sql, connection)
    if not df_customer.empty:
        dataframes_customer.append(df_customer)


    while True:
        offset += batch_size
        sql = f"SELECT customer_id,customer_name FROM t_customer LIMIT {batch_size} OFFSET {offset}"
        df_customer = pd.read_sql_query(sql, connection)

        if df_customer.empty:
            break
        dataframes_customer.append(df_customer)

    batch_size = 1000
    offset = 0


    sql = f"SELECT device_no,device_name,customer_id,branch_factory FROM t_device_wide LIMIT {batch_size}"
    df_device = pd.read_sql_query(sql, connection)
    if not df_device.empty:
        dataframes_device.append(df_device)


    while True:
        offset += batch_size
        sql = f"SELECT device_no,device_name,customer_id,branch_factory FROM t_device_wide LIMIT {batch_size} OFFSET {offset}"
        df_device = pd.read_sql_query(sql, connection)

        if df_device.empty:
            break
        dataframes_device.append(df_device)

    batch_size = 1000
    offset = 0


    sql = f"SELECT customer_id,device_detail_type,device_no,full_cycle_times,free_times,work_times,p_date FROM t_product_event_info LIMIT {batch_size}"
    df_event = pd.read_sql_query(sql, connection)
    if not df_event.empty:
        dataframes_event.append(df_event)


    while True:
        offset += batch_size
        sql = f"SELECT customer_id,device_detail_type,device_no,full_cycle_times,free_times,work_times,p_date FROM t_product_event_info LIMIT {batch_size} OFFSET {offset}"
        df_event = pd.read_sql_query(sql, connection)

        if df_event.empty:
            break
        dataframes_event.append(df_event)
finally:
    connection.close()

if dataframes_device:
    all_device_data = pd.concat(dataframes_device, ignore_index=False)
else:
    print("No data retrieved from the database.")

if dataframes_customer:
    all_customer_data = pd.concat(dataframes_customer, ignore_index=False)
else:
    print("No data retrieved from the database.")

if dataframes_event:
    all_event_data = pd.concat(dataframes_event, ignore_index=False)
else:
    print("No data retrieved from the database.")


from flask import Flask, jsonify, request

app = Flask(__name__)
@app.route('/api/customer_zhuanlu', methods=['GET'])
def get_data():
    try:
        starttime = request.args.get('starttime')
        endtime = request.args.get('endtime')

        filtered_df_device = all_device_data[all_device_data['device_name'].str.contains('转炉')]
        filtered_df_event = all_event_data[all_event_data['device_detail_type'].str.contains('转炉')]

        starttime = pd.to_datetime(starttime)
        endtime = pd.to_datetime(endtime)


        filtered_df_event = filtered_df_event.loc[
            (filtered_df_event['p_date'] >= starttime) & (filtered_df_event['p_date'] <= endtime)]

        result_df = filtered_df_event.groupby(['customer_id', 'device_no']).agg({
            'full_cycle_times': 'mean',
            'free_times': 'mean',
            'work_times': 'mean'
        }).reset_index()
        merged_df = result_df.merge(all_customer_data, on='customer_id')
        aaamerge_df = merged_df.merge(filtered_df_device, on='device_no')
        aaamerge_df = aaamerge_df.rename(columns={'customer_id_x': 'customer_id'})
        aaamerge_df = aaamerge_df.drop(columns=['device_name', 'customer_id_y'])

        json_dict = {column: aaamerge_df[column].tolist() for column in aaamerge_df.columns}
        return json_dict
    except:
        return '0'


if __name__ == '__main__':
    app.run(debug=True)









