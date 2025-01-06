
import pandas as pd
import numpy as np
from pymongo import MongoClient
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import pickle

client = MongoClient("mongodb+srv://pgmjo:aIdg0yUMUaZHyVN7@cluster0.noq3s.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")  # Modifier si nécessaire
db_weather = client["Weather"]  # Base météo
db_energy = client["daily_energy_load"]  # Base énergie
db_model=client["models"]
db_inference=client["inferences"]
weather_data = pd.DataFrame(list(db_weather["daily_weather_cities"].find()))
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


inference_data = weather_wide[(weather_wide['date'] >= pd.to_datetime('today').normalize())]

# Extract the `date` and `country_code` columns for the final output
metadata = inference_data[['date', 'country_code']]

# Drop unnecessary columns for inference
X_inference = inference_data.drop(columns=[ 'country_code', 'date'])

# Load the model from the database
model_document = db_model["trained_models"].find_one({"model_name": "random_forest_energy_prediction"})
model_binary = model_document["model_binary"]
loaded_model = pickle.loads(model_binary)

# Make predictions
predictions = loaded_model.predict(X_inference)

# Create the final dataframe with `date`, `country_code`, and predictions
results_df = metadata.copy()  # Start with the metadata
results_df['prediction'] = predictions  # Add predictions as a new column


from pymongo import UpdateOne
data_dict = results_df.to_dict("records")
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

collection=db_inference['inferences_data']
# Perform the bulk write operation
result = collection.bulk_write(update_operations)
# Output the result
print(f"Number of documents inserted: {result.upserted_count}")
print(f"Number of documents updated: {result.modified_count}")