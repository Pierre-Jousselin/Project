import time
from os.path import exists

import pandas as pd
from entsoe import EntsoePandasClient
import datetime

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo import UpdateOne

country_code_list=["SE_1","SE_2","SE_3","SE_4"]
entsoe_api_key = "22cb6d0f-5368-4495-95b0-3856c4bb6f7b"
mongo_password= "aIdg0yUMUaZHyVN7"


# Initialize the ENTSO-E client with the API key
client = EntsoePandasClient(api_key=entsoe_api_key)

# Set the desired date range
start_date = pd.Timestamp("20220101", tz="Europe/Berlin")
end_date = pd.Timestamp.now(tz="Europe/Berlin")
country_code_list=["SE_1","SE_2","SE_3","SE_4"]
for country_code in country_code_list:
    energy_load_data = pd.DataFrame()
    start_date = pd.Timestamp("20220101", tz="Europe/Berlin")
    # Process data in batches of 100 days to prevent overwhelming the API
    batch_end_date = start_date + pd.Timedelta(days=100)
    while start_date < end_date:
        print(f"Fetching data: {start_date} to {batch_end_date}")

        # Query daily energy load data
        load = client.query_load(country_code, start=start_date, end=batch_end_date)
        # Check if load data is valid
        if load is not None and not load.empty:
            # Resample hourly data to daily averages
            daily_load = load.resample("D").mean()
            daily_load = daily_load.reset_index()
            daily_load.columns = ["date", "load"]

            # Append to the main DataFrame
            energy_load_data = pd.concat([energy_load_data, daily_load], axis=0)

        else:
            print(f"No data fetched for {start_date} to {batch_end_date}.")

        # Move to the next batch
        start_date = batch_end_date + pd.Timedelta(days=1)
        batch_end_date = start_date + pd.Timedelta(days=100)
        if batch_end_date > end_date:
            batch_end_date = end_date

        # Prevent API rate-limiting issues
        time.sleep(5)

    energy_load_data['country_code']=country_code
    energy_load_data = energy_load_data.dropna().reset_index(drop=True)
    energy_load_data['date'] = energy_load_data['date'].astype(str).str.slice(0, 10)
    energy_load_data['date'] = pd.to_datetime(energy_load_data['date'], format='%Y-%m-%d')

    uri = "mongodb+srv://pgmjo:aIdg0yUMUaZHyVN7@cluster0.noq3s.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    # Create a new client and connect to the server
    client_mango = MongoClient(uri)
    db = client_mango["Energy"]  # Replace 'mydatabase' with your database name
    collection = db["daily_energy_load"]  # Replace 'mycollection' with your collection name
    data_dict = energy_load_data.to_dict("records")

    # Prepare the data by setting `date` as the `_id` field
    for record in data_dict:
        record['_id'] = str(record['date'])+record['country_code']  # Set `date` as the primary key

    # Insert or update the data in MongoDB
    update_operations = []
    for record in data_dict:
        update_operations.append(
            UpdateOne(
                {'_id': record['_id']},  # Match by `_id` which is now the `date`
                {'$set': record},
                upsert=True  # Insert if not exists
            )
        )

    # Perform the bulk write operation
    result = collection.bulk_write(update_operations)

    # Output the result
    print(f"Number of documents inserted: {result.upserted_count}")
    print(f"Number of documents updated: {result.modified_count}")