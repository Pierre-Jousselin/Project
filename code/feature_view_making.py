import pandas as pd
import numpy as np
from pymongo import MongoClient
from pymongo import UpdateOne

client = MongoClient("mongodb+srv://pgmjo:aIdg0yUMUaZHyVN7@cluster0.noq3s.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")  # Modifier si nécessaire
db_weather = client["Weather"]  # Base météo
db_energy = client["Energy"]  # Base énergie
db_feature_view=client["Feature_view"]

weather_data = pd.DataFrame(list(db_weather["daily_weather_cities"].find()))
energy_data = pd.DataFrame(list(db_energy["daily_energy_load"].find()))
weather_data['date'] = pd.to_datetime(weather_data['date'])
energy_data['date'] = pd.to_datetime(energy_data['date'])

weather_data['city_feature'] = weather_data['city'] + '_'

# Pivot the weather data to create a wide format
weather_wide = weather_data.pivot_table(
    index=['date', 'country_code'],  # Keep date and country_code as indices
    columns='city',  # Make each city a separate column
    values=[
        'temperature_2m_mean',
        'precipitation_sum',
        'wind_speed_10m_max',
        'wind_direction_10m_dominant'
    ]
)
weather_wide.columns = [f"{col[1]}_{col[0]}" for col in weather_wide.columns]
weather_wide.reset_index(inplace=True)

merged_data = pd.merge(
    energy_data,  # Energy data
    weather_wide,  # Wide-format weather data
    on=['date', 'country_code'],  # Join on date and country_code
    how='inner'  # Keep only matching rows
)

data_dict = merged_data.to_dict("records")
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
print(merged_data)

collection=db_feature_view['Energy_with_weather']
# Perform the bulk write operation
result = collection.bulk_write(update_operations)

# Output the result
print(f"Number of documents inserted: {result.upserted_count}")
print(f"Number of documents updated: {result.modified_count}")







