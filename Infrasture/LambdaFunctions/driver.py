try:
    import unzip_requirements
except ImportError:
    pass

try:
    import json, os, uuid, base64, boto3, datetime, decimal, random
    from datetime import datetime
    from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
    from decimal import Decimal
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

except Exception as e:
    print("Error ******* : {} ".format(e))

print('Loading function')


def unmarshall(dynamo_obj: dict) -> dict:
    """Convert a DynamoDB dict into a standard dict."""
    deserializer = TypeDeserializer()
    return {k: deserializer.deserialize(v) for k, v in dynamo_obj.items()}


def marshall(python_obj: dict) -> dict:
    """Convert a standard dict into a DynamoDB ."""
    serializer = TypeSerializer()
    return {k: serializer.serialize(v) for k, v in python_obj.items()}


class Datetime(object):
    @staticmethod
    def get_year_month_day():
        """
        Return Year month and day
        :return: str str str
        """
        dt = datetime.now()
        year = dt.year
        month = dt.month
        day = dt.day
        return year, month, day


def flatten_dict(data, parent_key="", sep="_"):
    """Flatten data into a single dict"""
    try:
        items = []
        for key, value in data.items():
            new_key = parent_key + sep + key if parent_key else key
            if type(value) == dict:
                items.extend(flatten_dict(value, new_key, sep=sep).items())
            else:
                items.append((new_key, value))
        return dict(items)
    except Exception as e:
        return {}


def dict_clean(items):
    result = {}
    for key, value in items:
        if value is None:
            value = "n/a"
        if value == "None":
            value = "n/a"
        if value == "null":
            value = "n/a"
        if len(str(value)) < 1:
            value = "n/a"
        result[key] = str(value)
    return result


class CustomJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(CustomJsonEncoder, self).default(obj)


def create_parquet_files(messages):
    df = pd.DataFrame(data=messages)

    # Convert the Pandas dataframe to an Arrow table
    table = pa.Table.from_pandas(df)

    # Write the Arrow table to a Parquet file in memory
    parquet_bytes = pa.BufferOutputStream()
    pq.write_table(table, parquet_bytes)

    # Upload the Parquet file to S3
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("DEV_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("DEV_SECRET_KEY"),
        region_name=os.getenv("DEV_REGION"),
    )

    dt = datetime.now()
    year = dt.year
    month = dt.month
    day = dt.day

    path = f"raw/table_name=drivers/year={year}/month={month}/day={day}/{uuid.uuid4().__str__()}.parquet"

    s3.put_object(
        Bucket=os.getenv("BUCKET"),
        Key=path,
        Body=parquet_bytes.getvalue().to_pybytes()
    )
    return 200


def lambda_handler(event, context):
    print("event", event)
    processed_messages = []

    for record in event['Records']:

        eventName = record.get("eventName")
        print("record", record)
        json_data = None

        if eventName.strip().lower() == "INSERT".lower():
            json_data = record.get("dynamodb").get("NewImage")

        if eventName.strip().lower() == "MODIFY".lower():
            json_data = record.get("dynamodb").get("NewImage")

        if eventName.strip().lower() == "REMOVE".lower():
            json_data = record.get("dynamodb").get("OldImage")

        print("json_data", json_data, type(json_data))

        if json_data is not None:
            json_data_unmarshal = unmarshall(json_data)
            print("json_data_unmarshal", json_data_unmarshal)

            year, month, day = Datetime.get_year_month_day()

            json_string = json.dumps(json_data_unmarshal, cls=CustomJsonEncoder)
            json_dict = json.loads(json_string)
            _final_processed_json = flatten_dict(json_dict)
            processed_messages.append(json_dict)

    if processed_messages != []:
        create_parquet_files(messages=processed_messages)

    return 'Successfully processed {} records.'.format(len(event['Records']))
