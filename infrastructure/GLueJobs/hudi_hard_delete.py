"""
Author : Soumil Nitin Shah
Email shahsoumil519@gmail.com
--additional-python-modules  | faker==11.3.0
--conf  |  spark.serializer=org.apache.spark.serializer.KryoSerializer  --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog --conf spark.sql.legacy.pathOptionBehavior.enabled=true --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension

--datalake-formats | hudi
"""

try:
    import sys, os, ast, uuid, boto3, datetime, time, re, json
    from ast import literal_eval
    from dataclasses import dataclass
    from datetime import datetime
    from pyspark.sql.functions import lit, udf
    from pyspark.sql.types import StringType
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
    from pyspark.sql.functions import *
    from awsglue.utils import getResolvedOptions
    from pyspark.sql.types import *
    import pandas as pd

except Exception as e:
    print("Modules are missing : {} ".format(e))

DYNAMODB_LOCK_TABLE_NAME = 'hudi-lock-table'
curr_session = boto3.session.Session()
curr_region = curr_session.region_name

# Get command-line arguments
args = getResolvedOptions(
    sys.argv, [
        'JOB_NAME',
        "HUDI_PATH",
        'TABLE_NAME',
        "HUDI_RECORD_ID_FIELD_NAME",
        "HUDI_RECORD_ID_VALUE",
        "PRECOMB_KEY",
        "PARTITION_PATH"
    ],
)

spark = (SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
         .config('spark.sql.hive.convertMetastoreParquet', 'false') \
         .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
         .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
         .config('spark.sql.legacy.pathOptionBehavior.enabled', 'true').getOrCreate())

# Create a Spark context and Glue context
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
logger = glueContext.get_logger()
job.init(args["JOB_NAME"], args)

path = args['HUDI_PATH']
table = args['TABLE_NAME']
type = 'delete'
record_id = args['HUDI_RECORD_ID_FIELD_NAME']
record_key_value = args['HUDI_RECORD_ID_VALUE']
precomb_key = args['PRECOMB_KEY']
partition_path = args['PARTITION_PATH']

print(f"""
path {path}
table {table}
record_id {record_id}
record_key_value {record_key_value}
precomb_key {precomb_key}
partition_path {partition_path}

""")

spark.read.format("hudi").load(path).createOrReplaceTempView("hudi_snapshots")
query = f"select * from hudi_snapshots  where  {record_id} = '{record_key_value}' "
print(query)
spark_df = spark.sql(query)
print(spark_df.show())

hudi_hard_delete_options = {
    'hoodie.table.name': table,
    'hoodie.datasource.write.recordkey.field': record_id,
    'hoodie.datasource.write.table.name': table,
    'hoodie.datasource.write.operation': 'delete',
    'hoodie.datasource.write.precombine.field': precomb_key,
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2,
    'hoodie.datasource.write.partitionpath.field': partition_path,

}
if spark_df.count() > 0:
    print("IN **")
    spark_df.write.format("hudi"). \
        options(**hudi_hard_delete_options). \
        mode("append"). \
        save(path)
