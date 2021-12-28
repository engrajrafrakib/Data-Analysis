from datetime import datetime
import mysql.connector
import pandas as pd

host_name = "****"
mysql_user_name = "****"
mysql_password = "****"
mysql_database_name = "****"

# creating connection to mysql database
mysql_connection = mysql.connector.connect(host=host_name, user=mysql_user_name, passwd=mysql_password, db=mysql_database_name)
mysql_cursor = mysql_connection.cursor()

query_to_create_mapping_dataset_table = "CREATE TABLE mapping_dataset (sensor_name varchar(200), " \
                                   "sensor_uuid varchar(200))"

start_time = datetime.now()
# Read mapping.csv file
mapping_dataframe = pd.read_csv("./dataset/mapping.csv", delimiter=';')

# preparing dataframe to insert into the database table
mapping_dataframe_values = mapping_dataframe.to_dict('records')

# create database table
mysql_cursor.execute("DROP TABLE IF EXISTS mapping_dataset")
mysql_cursor.execute(query_to_create_mapping_dataset_table)

# transfer data to database table
data_load_start = datetime.now()
mapping_dataset_insert_query = "INSERT INTO mapping_dataset (sensor_name, sensor_uuid) VALUES (%(sensor_name)s, %(sensor_uuid)s)"
mysql_cursor.executemany(mapping_dataset_insert_query, mapping_dataframe_values)
mysql_connection.commit()
data_load_end = datetime.now()
print('Data Insertion Time: {}'.format(data_load_end - data_load_start))
end_time = datetime.now()
print('Total Task Duration: {}'.format(end_time - start_time))
