import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1770859495015 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1770859495015")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1770859456950 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1770859456950")

# Script generated for node Transform SQL Query
SqlQuery1232 = '''
SELECT DISTINCT
    s.sensorReadingTime,
    s.serialNumber,
    s.distanceFromObject,
    a.x,
    a.y,
    a.z
FROM
    step_trainer_trusted s
JOIN
    accelerometer_trusted a
ON
    s.sensorReadingTime = a.timestamp
'''
TransformSQLQuery_node1770859520682 = sparkSqlQuery(glueContext, query = SqlQuery1232, mapping = {"accelerometer_trusted":accelerometer_trusted_node1770859495015, "step_trainer_trusted":step_trainer_trusted_node1770859456950}, transformation_ctx = "TransformSQLQuery_node1770859520682")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=TransformSQLQuery_node1770859520682, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1770857935419", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1770859973116 = glueContext.getSink(path="s3://arun-thiru/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1770859973116")
AmazonS3_node1770859973116.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="machine_learning_curated")
AmazonS3_node1770859973116.setFormat("json")
AmazonS3_node1770859973116.writeFrame(TransformSQLQuery_node1770859520682)
job.commit()