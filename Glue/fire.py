try:
    import datetime
    import json
    import random
    import boto3
    import os
    import uuid
    import time
    from datetime import datetime
    from faker import Faker
    from dotenv import load_dotenv

    load_dotenv("../Infrasture/.env")
except Exception as e:
    pass

global helper, BUCKET_NAME


BUCKET_NAME = "jt-soumilshah-test"
job_name = 'create_hudi_table_template'
glue = boto3.client(
    "glue",
    aws_access_key_id=os.getenv("DEV_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("DEV_SECRET_KEY"),
    region_name=os.getenv("DEV_AWS_REGION_NAME"),
)

payloads = [
    # {
    #     "jobName": "create_hudi_table_template",
    #     "glue_payload": {
    #         "JOB_NAME": "create_hudi_table_template-rides",
    #         "ENABLE_CLEANER": "True",
    #         "ENABLE_HIVE_SYNC": "True",
    #         "ENABLE_PARTITION": "False",
    #         "GLUE_DATABASE": "uber",
    #         "GLUE_TABLE_NAME": "rides",
    #         "HUDI_PRECOMB_KEY": "ride_id",
    #         "HUDI_RECORD_KEY": "ride_id",
    #         "HUDI_TABLE_TYPE": "COPY_ON_WRITE",
    #         'ENABLE_DYNAMODB_LOCK':"False",
    #         "PARTITON_FIELDS": "default",
    #         "SOURCE_FILE_TYPE": "parquet",
    #         "SOURCE_S3_PATH": f"s3://{BUCKET_NAME}/raw/table_name=rides/",
    #         "TARGET_S3_PATH": f"s3://{BUCKET_NAME}/silver/table_name=rides/",
    #         "INDEX_TYPE": "BLOOM",
    #         "USE_SQL_TRANSFORMER": "False",
    #         "SQL_TRANSFORMER_QUERY": "default"
    #     },
    # },
    {
        "jobName": "create_hudi_table_template",
        "glue_payload": {
            "JOB_NAME": "create_hudi_table_template-tips",
            "ENABLE_CLEANER": "True",
            "ENABLE_HIVE_SYNC": "True",
            "ENABLE_PARTITION": "False",
            "GLUE_DATABASE": "uber",
            "GLUE_TABLE_NAME": "tips",
            "HUDI_PRECOMB_KEY": "tip_id",
            "HUDI_RECORD_KEY": "tip_id",
            "HUDI_TABLE_TYPE": "COPY_ON_WRITE",
            "PARTITON_FIELDS": "default",
            'ENABLE_DYNAMODB_LOCK':"False",
            "SOURCE_FILE_TYPE": "parquet",
            "SOURCE_S3_PATH": f"s3://{BUCKET_NAME}/raw/table_name=tips/",
            "TARGET_S3_PATH": f"s3://{BUCKET_NAME}/silver/table_name=tips/",
            "INDEX_TYPE": "BLOOM",
            "USE_SQL_TRANSFORMER": "False",
            "SQL_TRANSFORMER_QUERY": "default"
        },
    },
    # {
    #     "jobName": "create_hudi_table_template",
    #     "glue_payload": {
    #         "JOB_NAME": "create_hudi_table_template-drivers",
    #         "ENABLE_CLEANER": "True",
    #         "ENABLE_HIVE_SYNC": "True",
    #         "ENABLE_PARTITION": "False",
    #         "GLUE_DATABASE": "uber",
    #         "GLUE_TABLE_NAME": "drivers",
    #         "HUDI_PRECOMB_KEY": "driver_id",
    #         "HUDI_RECORD_KEY": "driver_id",
    #         "HUDI_TABLE_TYPE": "COPY_ON_WRITE",
    #         "PARTITON_FIELDS": "default",
    #         'ENABLE_DYNAMODB_LOCK':"False",
    #         "SOURCE_FILE_TYPE": "parquet",
    #         "SOURCE_S3_PATH": f"s3://{BUCKET_NAME}/raw/table_name=drivers/",
    #         "TARGET_S3_PATH": f"s3://{BUCKET_NAME}/silver/table_name=drivers/",
    #         "INDEX_TYPE": "BLOOM",
    #         "USE_SQL_TRANSFORMER": "False",
    #         "SQL_TRANSFORMER_QUERY": "default"
    #     },
    # },
    # {
    #     "jobName": "create_hudi_table_template",
    #     "glue_payload": {
    #         "JOB_NAME": "create_hudi_table_template-users",
    #         "ENABLE_CLEANER": "True",
    #         "ENABLE_HIVE_SYNC": "True",
    #         "ENABLE_PARTITION": "False",
    #         "GLUE_DATABASE": "uber",
    #         "GLUE_TABLE_NAME": "users",
    #         "HUDI_PRECOMB_KEY": "user_id",
    #         "HUDI_RECORD_KEY": "user_id",
    #         "HUDI_TABLE_TYPE": "COPY_ON_WRITE",
    #         "PARTITON_FIELDS": "default",
    #         'ENABLE_DYNAMODB_LOCK':"False",
    #         "SOURCE_FILE_TYPE": "parquet",
    #         "SOURCE_S3_PATH": f"s3://{BUCKET_NAME}/raw/table_name=users/",
    #         "TARGET_S3_PATH": f"s3://{BUCKET_NAME}/silver/table_name=users/",
    #         "INDEX_TYPE": "BLOOM",
    #         "USE_SQL_TRANSFORMER": "False",
    #         "SQL_TRANSFORMER_QUERY": "default"
    #     },
    # },
]


fire = True
if fire:
    for payload in payloads:
        json_glue_payload = payload.get("glue_payload")

        fire_payload = {}
        for key, value in json_glue_payload.items(): fire_payload[f"--{key}"] = value

        response = glue.start_job_run(
            JobName=job_name,
            Arguments=fire_payload
        )
        print(response)
