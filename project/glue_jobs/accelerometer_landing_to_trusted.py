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

# Script generated for node customer_trusted
customer_trusted_node1770855308304 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="customer_trusted_node1770855308304")

# Script generated for node accelerometer_landing
accelerometer_landing_node1770853307209 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1770853307209")

# Script generated for node SQL Query
SqlQuery1288 = '''
SELECT DISTINCT
    a.user,
    a.timestamp,
    a.x,
    a.y,
    a.z
FROM
    accelerometer_landing a
JOIN
    customer_trusted c
ON
    a.user = c.email
'''
SQLQuery_node1770853649484 = sparkSqlQuery(glueContext, query = SqlQuery1288, mapping = {"accelerometer_landing":accelerometer_landing_node1770853307209, "customer_trusted":customer_trusted_node1770855308304}, transformation_ctx = "SQLQuery_node1770853649484")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1770853649484, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1770853297650", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1770854621943 = glueContext.getSink(path="s3://arun-thiru/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1770854621943")
AmazonS3_node1770854621943.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="accelerometer_trusted")
AmazonS3_node1770854621943.setFormat("json")
AmazonS3_node1770854621943.writeFrame(SQLQuery_node1770853649484)
job.commit()