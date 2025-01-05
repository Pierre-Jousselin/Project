import datetime
import pandas as pd
import os
from entsoe import EntsoePandasClient
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

entsoe_api_key = "22cb6d0f-5368-4495-95b0-3856c4bb6f7b"


mongo_password= "aIdg0yUMUaZHyVN7"
client = EntsoePandasClient(api_key=entsoe_api_key)
country_code = "SE_3"
energy_load_data = pd.DataFrame()

start_date = pd.Timestamp(
    datetime.datetime.now() + datetime.timedelta(days=-2), tz="Europe/Berlin"
)
end_date = pd.Timestamp(
    datetime.datetime.now() + datetime.timedelta(days=1), tz="Europe/Berlin"
)



load = client.query_load(country_code, start=start_date, end=end_date)
if load is not None and not load.empty:
            # Resample hourly data to daily averages
            daily_load = load.resample("D").mean()
            daily_load = daily_load.reset_index()
            daily_load.columns = ["date", "load"]

            # Append to the main DataFrame
            energy_load_data = pd.concat([energy_load_data, daily_load], axis=0)
energy_load_data['country_code']=country_code


energy_load_data['date'] = pd.to_datetime(energy_load_data['date'], format='%Y-%m-%d')
energy_load_data['date'] = pd.to_datetime(energy_load_data['date']).dt.date
energy_load_data['date'] = pd.to_datetime(energy_load_data['date'])
energy_load_data = energy_load_data.iloc[[-1]]


uri = "mongodb+srv://pgmjo:"+mongo_password+"@cluster0.noq3s.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
# Create a new client and connect to the server
client = MongoClient(uri)
db = client["daily_energy_load"]  # Replace 'mydatabase' with your database name
collection = db["S3"]  # Replace 'mycollection' with your collection name
data_dict = energy_load_data.to_dict("records")

# Insérer dans la collection MongoDB
result = collection.insert_many(data_dict)
