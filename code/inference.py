
import pandas as pd
import numpy as np
from pymongo import MongoClient
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import pickle

client = MongoClient("mongodb+srv://pgmjo:aIdg0yUMUaZHyVN7@cluster0.noq3s.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")  # Modifier si nécessaire
db_weather = client["Weather"]  # Base météo
db_energy = client["Energy"]  # Base énergie
db_model=client["models"]
db_inference=client["inferences"]
weather_data = pd.DataFrame(list(db_weather["daily_weather_cities"].find()))
country_code_list=["SE_1","SE_2","SE_3","SE_4"]
for country_code in country_code_list:
    weather_data_country=weather_data[weather_data['country_code']==country_code]
    weather_data_country['city_feature'] = weather_data_country['city'] + '_'

    # Pivot the weather data to create a wide format
    weather_wide = weather_data_country.pivot_table(
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
    model_document = db_model["trained_models"].find_one({"model_name": "random_forest_energy_prediction "+country_code})
    model_binary = model_document["model_binary"]
    loaded_model = pickle.loads(model_binary)
    

    # Make predictions
    trained_columns = loaded_model.feature_names_in_ if hasattr(loaded_model, 'feature_names_in_') else None

    if trained_columns is not None:
        # Ensure columns in X_inference match the trained model's column order
        X_inference = X_inference.reindex(columns=trained_columns, fill_value=0)

        # Print a message if there were missing columns filled with default values
        missing_columns = set(trained_columns) - set(X_inference.columns)
        if missing_columns:
            print(f"Warning: The following columns were missing in the inference data and filled with default values: {missing_columns}")
    predictions_inference = loaded_model.predict(X_inference)

    # Create the final dataframe with `date`, `country_code`, and predictions
    results_df = metadata.copy()  # Start with the metadata
    results_df['prediction'] = predictions_inference  # Add predictions as a new column


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


#########################################################################
