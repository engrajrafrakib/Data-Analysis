from datetime import datetime
import pandas as pd
import pandasql as ps
import mysql.connector
import pyodbc

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# mysql database connection
host_name = "****"
mysql_user_name = "****"
mysql_password = "****"
mysql_database_name = "****"

mysql_connection = mysql.connector.connect(host=host_name, user=mysql_user_name, passwd=mysql_password, db=mysql_database_name)
mysql_cursor = mysql_connection.cursor()

# mssql database connection
server_name = '****'
mssql_user_name = '****'
mssql_password = '****'
mssql_database_name = '****'

mssql_connection = pyodbc.connect("DRIVER={SQL SERVER};server=" + server_name + ";database=" + mssql_database_name + ";uid=" + mssql_user_name +";pwd=" + mssql_password)
mssql_cursor = mssql_connection.cursor()

# get user input
sensor_name = input('sensor_name: ')
start_timestamp = input('start_timestamp: ')
end_timestamp = input('end_timestamp: ')

# dataframe preparation
start_time = datetime.now()
mapping_df = pd.read_sql("SELECT * FROM mapping_dataset WHERE sensor_name= '" + sensor_name + "'", mysql_connection)
sensor_id = mapping_df[mapping_df['sensor_name'] == sensor_name]['sensor_uuid'].values[0]
sensor_df = pd.read_sql("SELECT * FROM dbo.sensor_dataset WHERE sensor_uuid = '" + sensor_id + "' AND TIMESTAMP BETWEEN '" + start_timestamp + "' AND '" + end_timestamp +"'", mssql_connection)

# query to get specific data
query = "SELECT mapping_df.sensor_name, mapping_df.sensor_uuid, sensor_df.timestamp, sensor_df.sensor_value FROM mapping_df " \
        "INNER JOIN sensor_df ON mapping_df.sensor_uuid=sensor_df.sensor_uuid WHERE mapping_df.sensor_uuid= '" \
        + sensor_id + "' AND sensor_df.timestamp BETWEEN '" + start_timestamp + "' AND '" + end_timestamp + "'"

result_df = ps.sqldf(query, locals())
end_time = datetime.now()
print('Total Task Duration: {}'.format(end_time - start_time))