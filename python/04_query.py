import pandas as pd
import os
os.chdir('./python/Classes')
from influxdb import *
import influxdb_client.client.influxdb_client


client = influxDB('lTUKuRE46dJw8Yj_AmYtQHELsnfNM1eGVdJkYUj_Q_Ddq7yqCScDlbt9PYdu-RR_OW-NX9S_GaxNqXz7iAECCw==', 
'my-org', 
'8086')

queryAPI = client.query_api()

#create flux query
myquery_location = 'from(bucket: "air-quality") |> range(start: 2013-03-25T00:00:00Z, stop: 2013-05-01T00:00:00Z)' \
            '|> filter(fn: (r) => r["_measurement"] == "location-tag-only")' \
            '|> filter(fn: (r) => r["_field"] == "TEMP")' 

location_df = client.execute_batchQuery(myquery_location, 'dataframe')

print(location_df.info())
print(location_df)


myquery_everything = 'from(bucket: "air-quality") |> range(start: 2013-03-25T00:00:00Z, stop: 2013-05-01T00:00:00Z)' \
            '|> filter(fn: (r) => r["_measurement"] == "full-tags")' \
            '|> filter(fn: (r) => r["_field"] == "TEMP")' 


everything_df = client.execute_batchQuery(myquery_everything, 'dataframe')

print(everything_df)