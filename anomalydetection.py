from datetime import datetime
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import sys


def format_plot_data(sensor_dataframe):
    sensor_ts = []
    sensor_values = []
    total_rows = sensor_dataframe.shape[0]
    count = 0
    format_start_time = datetime.now()
    df_dict = sensor_dataframe.to_dict('records')
    for row in df_dict:
        count += 1
        sensor_ts.append(matplotlib.dates.datestr2num(row['timestamp']))
        sensor_values.append(float(row['sensor_value']))
        if count % 5000 == 0:
            sys.stdout.write("\rData formatted %d out of %d" % (count, total_rows))
            #sys.stdout.flush()
    print("\ncomplete formatted data")
    format_end_time = datetime.now()
    print("\ndata format time: ", format_end_time - format_start_time)
    return sensor_ts, sensor_values


start_time = datetime.now()
print("reading from CSV files...")
sensor_dataframe = pd.read_csv('./dataset/data.csv', delimiter=';')

# # show percentile to get idea of anomalies
# sensor_dataframe = sensor_dataframe[:100000]
print(sensor_dataframe.describe(percentiles=[0.25,0.5,0.75,0.9,0.95,0.98,0.99]))

# format data for plotting anomalies detection
sensor_ts, sensor_values = format_plot_data(sensor_dataframe)

# draw plot
plt.rcParams["figure.figsize"] = (40,5)
plt.xlabel('Time')
plt.ylabel('Value')
plt.plot_date(sensor_ts, sensor_values, '-')
plt.savefig('../images/check_outlires.png')
# plt.show()

# remove outlires
clean_df = sensor_dataframe[(sensor_dataframe.sensor_value >= -1100) & (sensor_dataframe.sensor_value <= 1100)]

# format clean data for plotting anomalies detection
sensor_ts, sensor_values = format_plot_data(clean_df)

# draw plot
plt.rcParams["figure.figsize"] = (40,5)
plt.xlabel('Time')
plt.ylabel('Value')
plt.plot_date(sensor_ts, sensor_values, '-')
plt.savefig('../images/wihtout_outlires.png')
# plt.show()
end_time = datetime.now()
print('Total Task Duration: {}'.format(end_time - start_time))
