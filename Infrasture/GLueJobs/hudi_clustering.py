"""Soumil Shah"""
try:
    import sys
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.sql.types import *
except Exception as e:
    print("Modules are missing: {}".format(e))


# Get command-line arguments
args = getResolvedOptions(
    sys.argv, [
        'JOB_NAME',
        'GLUE_DATABASE',
        'GLUE_TABLE_NAME'
    ],
)

# Create a Spark session and Glue context
spark = (SparkSession.builder.config(
    'spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
         .config('spark.sql.hive.convertMetastoreParquet', 'false') \
         .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
         .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
         .config('spark.sql.legacy.pathOptionBehavior.enabled', 'true')
         .getOrCreate())

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
logger = glueContext.get_logger()
job.init(args['JOB_NAME'], args)

# Define the database and table names
db_name = args['GLUE_DATABASE']
table_name = args['GLUE_TABLE_NAME']

query_show_commits = f"call show_commits('{db_name}.{table_name}', 5)"
spark_df_commits = spark.sql(query_show_commits)
spark_df_commits.show()

# Execute clustering
query_show_clustering = f"call run_clustering('{db_name}.{table_name}')"
spark_df_clusterings = spark.sql(query_show_clustering)
spark_df_clusterings.show()