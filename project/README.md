# STEDI Human Balance Analytics — Data Lake Project

## Overview
This project builds a simple data pipeline on AWS to help STEDI analyze human balance.  
We take raw customer and sensor data, clean it, protect private information, and prepare it for machine‑learning.

The pipeline uses Amazon S3 for storage, AWS Glue for ETL jobs, and Athena to check the results.

---

## How the Data Moves
The data flows through three zones:

1. **Landing Zone** – raw data exactly as received
2. **Trusted Zone** – cleaned data with privacy rules applied
3. **Curated Zone** – final datasets ready for machine learning

---

## Privacy Filtering
STEDI only wants to use data from customers who agreed to share their information for research.

In the first Glue job, we remove any customer who does **not** have a value in `shareWithResearchAsOfDate`.

This ensures only approved customer data moves forward.

---

## ETL Jobs (Simple Summary)

### 1. customer_landing_to_trusted
- Removes customers who didn’t consent.
- Output: `customer_trusted` (482 rows)

### 2. accelerometer_landing_to_trusted
- Keeps accelerometer readings only for trusted customers.
- Output: `accelerometer_trusted` (40,981 rows)

### 3. step_trainer_landing_to_trusted
- Keeps step trainer readings only for trusted customers.
- Output: `step_trainer_trusted` (14,460 rows)

### 4. customer_trusted_to_curated
- Creates the final customer table for ML.
- Output: `customer_curated` (482 rows)

### 5. machine_learning_curated
- Combines step trainer + accelerometer data for ML training.
- Output: `machine_learning_curated` (43,681 rows)

---

## Validation (Athena Row Counts)
All tables match the expected counts:

- customer_trusted: **482**
- accelerometer_trusted: **40,981**
- step_trainer_trusted: **14,460**
- customer_curated: **482**
- machine_learning_curated: **43,681**

---

## What This Project Shows
- How to build a simple data lake on AWS
- How to clean and filter data using Glue
- How to protect private information
- How to prepare datasets for machine learning

This completes the full STEDI data pipeline from raw data to ML‑ready output.