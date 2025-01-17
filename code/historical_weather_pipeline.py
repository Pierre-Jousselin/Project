import os
import datetime
import time
import requests
import pandas as pd
import json
from geopy.geocoders import Nominatim
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.ticker import MultipleLocator
import openmeteo_requests
import requests_cache
from retry_requests import retry

SE_dict = {"SE_1":{"Lulea":{"latitude":65.585220558314, "longitude":22.1535400967245},
                  "Kiruna":{"latitude":67.8597228129746, "longitude":20.2820265311514},
                  "Gallivare":{"latitude":67.1378762455943, "longitude":20.6603243600605},
                  "Jokkmokk":{"latitude":66.606896356426, "longitude":19.8238206662249}},
           "SE_2":{"Sundsvall":{"latitude":62.3922563843034, "longitude":17.3021441711501},
                  "Are":{"latitude":63.398913630157, "longitude":13.0820017348496},
                  "Umea":{"latitude":63.8249317320017, "longitude":20.2591261458924},
                  "Stromsund":{"latitude":63.8501774561328, "longitude":15.5509119891757}},
           "SE_3":{"Stockholm":{"latitude":59.3325756755426, "longitude":18.075853780289},
                  "Gothenburg":{"latitude":57.7085886694326, "longitude":11.9460144037959},
                  "Linkoping":{"latitude":58.4105966645195, "longitude":15.623474706219},
                  "Gotland":{"latitude":57.4679159436546, "longitude":18.4815612941467}},
           "SE_4":{"Malmo":{"latitude":55.6070050280266, "longitude":13.0022375574572},
                  "Lund":{"latitude":55.7048756026586, "longitude":13.1918157047214},
                  "Vaxjo":{"latitude":56.879630751228, "longitude":14.8058126585592},
                  "Halmstad":{"latitude":56.6680127474774, "longitude":12.8664883710698}}}

def get_historical_weather(city, start_date,  end_date, latitude, longitude):
    # latitude, longitude = get_city_coordinates(city)

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ["temperature_2m_mean", "precipitation_sum", "wind_speed_10m_max", "wind_direction_10m_dominant"]
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process daily data. The order of variables needs to be the same as requested.
    daily = response.Daily()
    daily_temperature_2m_mean = daily.Variables(0).ValuesAsNumpy()
    daily_precipitation_sum = daily.Variables(1).ValuesAsNumpy()
    daily_wind_speed_10m_max = daily.Variables(2).ValuesAsNumpy()
    daily_wind_direction_10m_dominant = daily.Variables(3).ValuesAsNumpy()

    daily_data = {"date": pd.date_range(
        start = pd.to_datetime(daily.Time(), unit = "s"),
        end = pd.to_datetime(daily.TimeEnd(), unit = "s"),
        freq = pd.Timedelta(seconds = daily.Interval()),
        inclusive = "left"
    )}
    daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
    daily_data["precipitation_sum"] = daily_precipitation_sum
    daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
    daily_data["wind_direction_10m_dominant"] = daily_wind_direction_10m_dominant

    daily_dataframe = pd.DataFrame(data = daily_data)
    daily_dataframe = daily_dataframe.dropna()
    daily_dataframe['city'] = city
    return daily_dataframe

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
uri = "mongodb+srv://pgmjo:aIdg0yUMUaZHyVN7@cluster0.noq3s.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
# Create a new client and connect to the server
client = MongoClient(uri)
# Send a ping to confirm a successful connection


db = client["Weather"]  
collection = db["daily_weather_cities"]  # Replace 'mycollection' with your collection name

from pymongo import UpdateOne
def upload(df):
    data_dict = df.to_dict("records")
    # Prepare the data by setting `date` as the `_id` field
    for record in data_dict:
        record['_id'] = str(record['date'])+record['city']  # Set `date` as the primary key

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

start_date = "2022-01-01"
end_date = "2025-01-03"

try:
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y-%m-%d")
except ValueError:
    raise ValueError("Dates must be in 'YYYY-MM-DD' format.")

for key, inner_dict in SE_dict.items():
    for inner_key, city_dict in inner_dict.items():
        city_dict["historical_df"]= get_historical_weather(inner_key, start_date,  end_date, city_dict["latitude"], city_dict["longitude"])
        city_dict["historical_df"]['country_code']=key
        upload(city_dict["historical_df"])