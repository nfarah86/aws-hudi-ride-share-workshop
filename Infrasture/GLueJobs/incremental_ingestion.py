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
        'BUCKET_NAME',
        'DATABASE_NAME'
    ],
)

global BUCKET_NAME
BUCKET_NAME = args['BUCKET_NAME']

rides_path = f"s3://{BUCKET_NAME}/silver/table_name=rides/"
tips_path = f"s3://{BUCKET_NAME}/silver/table_name=tips/"
driver_earnings = f"s3://{BUCKET_NAME}/gold/table_name=driver_earnings/"
date_dimensions = f"s3://{BUCKET_NAME}/silver/table_name=dim_date/"

spark = (SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
         .config('spark.sql.hive.convertMetastoreParquet', 'false') \
         .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
         .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
         .config('spark.sql.legacy.pathOptionBehavior.enabled', 'true').getOrCreate())

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
logger = glueContext.get_logger()
job.init(args["JOB_NAME"], args)


class AWSS3(object):
    """Helper class to which add functionality on top of boto3 """

    def __init__(self, bucket):

        self.BucketName = bucket
        self.client = boto3.client("s3")

    def put_files(self, Response=None, Key=None):
        """
        Put the File on S3
        :return: Bool
        """
        try:
            response = self.client.put_object(
                Body=Response, Bucket=self.BucketName, Key=Key
            )
            return "ok"
        except Exception as e:
            raise Exception("Error : {} ".format(e))

    def item_exists(self, Key):
        """Given key check if the items exists on AWS S3 """
        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return True
        except Exception as e:
            return False

    def get_item(self, Key):

        """Gets the Bytes Data from AWS S3 """

        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return response_new["Body"].read()

        except Exception as e:
            print("Error :{}".format(e))
            return False

    def find_one_update(self, data=None, key=None):

        """
        This checks if Key is on S3 if it is return the data from s3
        else store on s3 and return it
        """

        flag = self.item_exists(Key=key)

        if flag:
            data = self.get_item(Key=key)
            return data

        else:
            self.put_files(Key=key, Response=data)
            return data

    def delete_object(self, Key):

        response = self.client.delete_object(Bucket=self.BucketName, Key=Key, )
        return response

    def get_all_keys(self, Prefix=""):

        """
        :param Prefix: Prefix string
        :return: Keys List
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.BucketName, Prefix=Prefix)

            tmp = []

            for page in pages:
                for obj in page["Contents"]:
                    tmp.append(obj["Key"])

            return tmp
        except Exception as e:
            return []

    def print_tree(self):
        keys = self.get_all_keys()
        for key in keys:
            print(key)
        return None

    def find_one_similar_key(self, searchTerm=""):
        keys = self.get_all_keys()
        return [key for key in keys if re.search(searchTerm, key)]

    def __repr__(self):
        return "AWS S3 Helper class "


@dataclass
class HUDISettings:
    """Class for keeping track of an item in inventory."""

    table_name: str
    path: str


class HUDIIncrementalReader(AWSS3):
    def __init__(self, bucket, hudi_settings, spark_session):
        AWSS3.__init__(self, bucket=bucket)
        if type(hudi_settings).__name__ != "HUDISettings": raise Exception("please pass correct settings ")
        self.hudi_settings = hudi_settings
        self.spark = spark_session

    def __check_meta_data_file(self):
        """
        check if metadata for table exists
        :return: Bool
        """
        file_name = f"metadata/{self.hudi_settings.table_name}.json"
        return self.item_exists(Key=file_name)

    def __read_meta_data(self):
        file_name = f"metadata/{self.hudi_settings.table_name}.json"

        return ast.literal_eval(self.get_item(Key=file_name).decode("utf-8"))

    def __push_meta_data(self, json_data):
        file_name = f"metadata/{self.hudi_settings.table_name}.json"
        self.put_files(
            Key=file_name, Response=json.dumps(json_data)
        )

    def clean_check_point(self):
        file_name = f"metadata/{self.hudi_settings.table_name}.json"
        self.delete_object(Key=file_name)

    def __get_begin_commit(self):
        self.spark.read.format("hudi").load(self.hudi_settings.path).createOrReplaceTempView("hudi_snapshot")
        commits = list(map(lambda row: row[0], self.spark.sql(
            "select distinct(_hoodie_commit_time) as commitTime from  hudi_snapshot order by commitTime asc").limit(
            50).collect()))

        """begin from start """
        begin_time = int(commits[0]) - 1
        return begin_time

    def __read_inc_data(self, commit_time):
        incremental_read_options = {
            'hoodie.datasource.query.type': 'incremental',
            'hoodie.datasource.read.begin.instanttime': commit_time,
        }
        incremental_df = self.spark.read.format("hudi").options(**incremental_read_options).load(
            self.hudi_settings.path).createOrReplaceTempView("hudi_incremental")

        df = self.spark.sql("select * from  hudi_incremental")

        return df

    def __get_last_commit(self):
        commits = list(map(lambda row: row[0], self.spark.sql(
            "select distinct(_hoodie_commit_time) as commitTime from  hudi_incremental order by commitTime asc").limit(
            50).collect()))
        last_commit = commits[len(commits) - 1]
        return last_commit

    def __run(self):
        """Check the metadata file"""
        flag = self.__check_meta_data_file()
        """if metadata files exists load the last commit and start inc loading from that commit """
        if flag:
            meta_data = json.loads(self.__read_meta_data())
            print(f"""
            ******************LOGS******************
            meta_data {meta_data}
            last_processed_commit : {meta_data.get("last_processed_commit")}
            ***************************************
            """)

            read_commit = str(meta_data.get("last_processed_commit"))
            df = self.__read_inc_data(commit_time=read_commit)

            """if there is no INC data then it return Empty DF """
            if not df.rdd.isEmpty():
                last_commit = self.__get_last_commit()
                self.__push_meta_data(json_data=json.dumps({
                    "last_processed_commit": last_commit,
                    "table_name": self.hudi_settings.table_name,
                    "path": self.hudi_settings.path,
                    "inserted_time": datetime.now().__str__(),

                }))
                return df
            else:
                return df

        else:

            """Metadata files does not exists meaning we need to create  metadata file on S3 and start reading from begining commit"""

            read_commit = self.__get_begin_commit()

            df = self.__read_inc_data(commit_time=read_commit)
            last_commit = self.__get_last_commit()

            self.__push_meta_data(json_data=json.dumps({
                "last_processed_commit": last_commit,
                "table_name": self.hudi_settings.table_name,
                "path": self.hudi_settings.path,
                "inserted_time": datetime.now().__str__(),

            }))

            return df

    def read(self):
        """
        reads INC data and return Spark Df
        :return:
        """

        return self.__run()


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
    earning_fact = f"s3://{BUCKET_NAME}/gold/table_name=driver_earnings/"

    spark_df.write.format("hudi"). \
        options(**hudi_final_settings). \
        mode("append"). \
        save(earning_fact)


rides_helper = HUDIIncrementalReader(
    bucket=BUCKET_NAME,
    hudi_settings=HUDISettings(
        table_name='rides',
        path=rides_path
    ),
    spark_session=spark
)

tips_helper = HUDIIncrementalReader(
    bucket=BUCKET_NAME,
    hudi_settings=HUDISettings(
        table_name='tips',
        path=tips_path
    ),
    spark_session=spark
)

inc_rides_df = rides_helper.read()
print("inc_rides_df")
print(inc_rides_df.show(2))

inc_tips_df = tips_helper.read()
print("inc_tips_df")
print(inc_tips_df.show(2))

inc_rides_df.createOrReplaceTempView("rides")
inc_tips_df.createOrReplaceTempView("tips")

# ==============DATE DIM ======================================================================================
spark.read.format("hudi").load(date_dimensions).createOrReplaceTempView("date_dim")
# ================================================================================================================


if inc_rides_df.count() > 0:
    earning_fact_df = spark.sql("""
    SELECT
        r.driver_id,
        r.ride_id,
        r.fare,
        COALESCE(t.tip_amount, 0) AS tip_amount,
        (r.fare + COALESCE(t.tip_amount, 0)) AS total_amount,
        d.date_key as earning_date_key
    FROM
        rides r
    LEFT JOIN
        tips t ON r.ride_id = t.ride_id
    JOIN (
            SELECT
                date_key,
                to_date(date_key, 'yyyyMMdd') as date
            FROM
                date_dim
            ) d ON to_date(r.ride_date, 'yyyy-MM-dd') = d.date
""")

    upsert_hudi_table(
        db_name="uber",
        table_name="driver_earnings",
        record_id="driver_id,ride_id",
        precomb_key="ride_id",
        spark_df=earning_fact_df,
        table_type='COPY_ON_WRITE',
        method='upsert',
    )

if inc_rides_df.count() == 0 and inc_tips_df.count() > 0:
    spark.read.format("hudi").load(driver_earnings).createOrReplaceTempView("driver_earnings")

    earning_fact_new_df = spark.sql("""
        SELECT
            de.driver_id,
            de.ride_id,
            de.fare,
            COALESCE(t.tip_amount, 0) AS tip_amount,
            (de.fare + COALESCE(t.tip_amount, 0)) AS total_amount,
            de.earning_date_key
        FROM
            driver_earnings de
        JOIN
            tips t ON de.ride_id = t.ride_id
    """)

    upsert_hudi_table(
        db_name=args['DATABASE_NAME'],
        table_name="driver_earnings",
        record_id="driver_id,ride_id",
        precomb_key="ride_id",
        spark_df=earning_fact_new_df,
        table_type='COPY_ON_WRITE',
        method='upsert',
    )
