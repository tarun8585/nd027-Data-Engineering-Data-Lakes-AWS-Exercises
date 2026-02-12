CREATE EXTERNAL TABLE IF NOT EXISTS stedi_db.step_trainer_landing (
    sensorReadingTime bigint,
    serialNumber string,
    distanceFromObject double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://arun-thiru/step_trainer/landing/';
