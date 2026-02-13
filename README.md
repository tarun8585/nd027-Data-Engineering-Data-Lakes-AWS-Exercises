# STEDI Data Pipeline – Short README

This project builds a small data pipeline that prepares sensor data for a machine‑learning model. The data comes from three sources: customer data, accelerometer data, and step trainer data. The goal is to clean the data, filter out customers who did not agree to share their information, and then combine the sensor readings so they can be used for training a model.

## 1. customer_trusted
We start with the raw customer landing data. We keep only customers who agreed to share their data (shareWithResearchAsOfDate is not blank). This becomes customer_trusted.

## 2. accelerometer_trusted
Next, we join accelerometer readings with customer_trusted. This keeps only accelerometer data from customers who opted in. This becomes accelerometer_trusted.

## 3. customer_curated
From the trusted customers, we keep only those who actually have accelerometer readings. This ensures we only keep active customers with real sensor data. This becomes customer_curated.

## 4. step_trainer_trusted
We filter step trainer readings to only the curated customers (using serial numbers). This removes any devices that do not belong to curated customers. This becomes step_trainer_trusted.

## 5. machine_learning_curated
Finally, we join step_trainer_trusted, accelerometer_trusted, and customer_curated. We match them on timestamp to create a clean dataset for machine learning. This becomes machine_learning_curated.

## Pipeline Summary
1. Filter customers who agreed to share data
2. Keep accelerometer readings from those customers
3. Keep only customers who have accelerometer data
4. Keep step trainer readings from those customers
5. Join everything together for ML
