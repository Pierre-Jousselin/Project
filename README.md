# Energy Load Prediction Using Weather Data in Sweden

This repository contains the implementation of a project aimed at predicting energy load in Sweden using historical and forecasted weather data. The project focuses on improving the efficiency of energy management and sustainability practices by leveraging machine learning techniques.



![Project Summary Diagram](diagrame.png)

## Features

- **Data Sources**: 
  - **Energy Load Data**: Sourced from the ENTSO-E Transparency Platform.
  - **Weather Data**: Historical and forecast data retrieved using the Open-Meteo API.
- **Storage**: MongoDB used for data management with separate databases for energy, weather, features, and predictions.
- **Modeling**: 
  - Four region-specific Random Forest models trained to account for variations in energy consumption.
  - Hyperparameter tuning using Grid Search.
- **Daily Pipelines**:
  - Automated retrieval and updates for energy and weather data.
- **Predictions and Monitoring**:
  - 10-day energy load predictions stored in the database.
  - Performance tracked using metrics like MAE, MSE, and R² score.

## Methodology

1. **Data Collection**:
   - Energy data aggregated to daily values for consistency.
   - Weather data collected for four cities per region, capturing temperature, wind speed, precipitation, etc.

2. **Data Management**:
   - Databases designed for weather, energy, and feature views.
   - Prediction database for monitoring inference results.

3. **Model Training**:
   - Separate models for SE1, SE2, SE3, and SE4 to account for regional differences.
   - Random Forest models trained with weather and energy data features.

4. **Visualization**:
   - Dashboard for real-time monitoring and visualization of predictions.

## Dependencies

- Python (version >= 3.8)
- MongoDB
- Scikit-learn
- Pandas
- NumPy
- Open-Meteo API
- ENTSO-E API




