CREATE EXTERNAL TABLE IF NOT EXISTS stedi_db.accelerometer_landing (
    user string,
    timestamp bigint,
    x double,
    y double,
    z double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://arun-thiru/accelerometer/landing/';