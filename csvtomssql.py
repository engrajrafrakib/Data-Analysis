from datetime import datetime
import pandas as pd
import pyodbc

query_to_create_data_dataset_table = '''CREATE TABLE sensor_dataset(timestamp varchar(200), sensor_uuid varchar(200), sensor_value float)'''

# creating connection to mssql database
server_name = '****'
mssql_user_name = '****'
mssql_password = '****'
mssql_database_name = '****'

mssql_connection = pyodbc.connect("DRIVER={SQL SERVER};server=" + server_name + ";database=" + mssql_database_name + ";uid=" + mssql_user_name +";pwd=" + mssql_password)
mssql_cursor = mssql_connection.cursor()
mssql_cursor.fast_executemany = True

start_time = datetime.now()
# Read data.csv file
sensor_dataframe = pd.read_csv("./dataset/data.csv", delimiter=';')
# sensor_dataframe = sensor_dataframe[:1000000]

# creating list of tuple(row) to insert into the database table
sensor_dataframe_values = list(tuple(row) for row in sensor_dataframe.values)

# create database table
mssql_cursor.execute("DROP TABLE IF EXISTS dbo.sensor_dataset")
mssql_cursor.execute(query_to_create_data_dataset_table)

# transfer data to database table
data_load_start = datetime.now()
sensor_data_insert_query = "INSERT INTO dbo.sensor_dataset (timestamp, sensor_uuid, sensor_value) VALUES (?,?,?)"

for i in range(0, len(sensor_dataframe_values), 100000):
    chunk = sensor_dataframe_values[i:i + 100000]
    mssql_cursor.executemany(sensor_data_insert_query, chunk)
    mssql_connection.commit()
data_load_end = datetime.now()
print('Data Insertion Time: {}'.format(data_load_end - data_load_start))
end_time = datetime.now()
print('Total Task Duration: {}'.format(end_time - start_time))