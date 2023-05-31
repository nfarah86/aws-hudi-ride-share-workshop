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
        'BUCKET_NAME'
    ],
)

global BUCKET_NAME
BUCKET_NAME = args['BUCKET_NAME']

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

def upsert_hudi_table(
        db_name,
        table_name,
        record_id,
        precomb_key,
        spark_df,
        table_type='COPY_ON_WRITE',
        method='upsert',

):
    hudi_final_settings = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": table_type,
        "hoodie.datasource.write.operation": method,
        "hoodie.datasource.write.recordkey.field": record_id,
        "hoodie.datasource.write.precombine.field": precomb_key,

    }

    hudi_hive_sync_options = {
        "hoodie.datasource.hive_sync.database": db_name,
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.parquet.compression.codec": "gzip",
        "hoodie.datasource.hive_sync.mode": "hms",
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",

    }

    hudi_cleaner_options = {
        "hoodie.clean.automatic": "true"
        , "hoodie.clean.async": "true"
        , "hoodie.cleaner.policy": 'KEEP_LATEST_FILE_VERSIONS'
        , "hoodie.cleaner.fileversions.retained": "3"
        , "hoodie-conf hoodie.cleaner.parallelism": '200'
        , 'hoodie.cleaner.commits.retained': 5

    }

    for key, value in hudi_hive_sync_options.items(): hudi_final_settings[key] = value
    earning_fact = f"s3://{BUCKET_NAME}/silver/table_name=dim_date/"

    spark_df.write.format("hudi"). \
        options(**hudi_final_settings). \
        mode("append"). \
        save(earning_fact)


# ==============DATE DIM ======================================================================================
min_date = '2000-01-01'
max_date = '2025-01-01'
date_range = pd.date_range(start=min_date, end=max_date)
date_data = [(int(day.strftime('%Y%m%d')), day.year, day.month, day.day, str((day.month - 1) // 3 + 1),
              day.strftime('%A'), day.weekday()) for day in date_range]
date_schema = ['date_key', 'year', 'month', 'day', 'quarter', 'weekday', 'weekday_number']
date_dim_df = spark.createDataFrame(date_data, schema=date_schema)

# ================================================================================================================

upsert_hudi_table(
    db_name="uber",
    table_name="dim_date",
    record_id="date_key",
    precomb_key="date_key",
    spark_df=date_dim_df,
    table_type='COPY_ON_WRITE',
    method='upsert',
)
