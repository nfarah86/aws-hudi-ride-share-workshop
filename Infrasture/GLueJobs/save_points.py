try:
    import sys
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
    from pyspark.sql.functions import *
    from awsglue.utils import getResolvedOptions
    from pyspark.sql.types import *
    from datetime import datetime, date
    import boto3
    from functools import reduce
    from pyspark.sql import Row

    import uuid
    from faker import Faker
except Exception as e:
    print("Modules are missing : {} ".format(e))

# Get command-line arguments
args = getResolvedOptions(
    sys.argv, [
        'JOB_NAME',
        'GLUE_DATABASE',
        'GLUE_TABLE_NAME'
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

db_name = args['GLUE_DATABASE']
table_name = args['GLUE_TABLE_NAME']

query_show_commits = f"call show_commits('{db_name}.{table_name}', 5)"
spark_df_commits = spark.sql(query_show_commits)
commits = list(map(lambda row: row[0], spark_df_commits.collect()))

query_save_point = f"call create_savepoint('{db_name}.{table_name}', '{commits[0]}')"
execute_save_point = spark.sql(query_save_point)

show_check_points_query = f"call show_savepoints('{db_name}.{table_name}')"
show_check_points_query_df = spark.sql(show_check_points_query)

print(f"""
**************************STATS**********************************
query {query_show_commits}
spark_df {spark_df_commits.show()}
commits {commits}
Latest commit: {commits[0]}

########################### Save Points ##########################
query:  {query_save_point}
save_point {execute_save_point.show()}

##################### ###### SHOW CHECK POINT ######################
query:  {show_check_points_query}
show_check_points_query_df {show_check_points_query_df.show()}
*********************************************************************
""")
