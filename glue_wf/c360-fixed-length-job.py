import os
import re
import sys
import json
import time
import uuid
import boto3
import streamlit as st
import requests as req
from io import StringIO
from xml.dom import minidom
# from awsglue.job import Job
# from awsglue.context import GlueContext
# from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import when, length, trim, col, lit, substring, concat_ws, udf, struct, split, monotonically_increasing_id, desc
from pyspark.sql.types import StringType, IntegerType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

args = {
    "JOB_NAME": "glue-fixed-length-job",
    "WORKFLOW_NAME": "c360-workflow",
    "WORKFLOW_RUN_ID": "20e134a2-3de7-4c45-8286-b1cecfc01f27",
    "EVENT_BUS_NAME": "customer360_ebus_dev"
}

WORKFLOW_PARAMS = {
    "REGION": "us-east-1",
    "VENDOR": "TECHM",
    "FILE_ROW_LENGTH": 707,
    "FILE_NAME": "Daily_New_RDR_File_20220217.txt",
    "FILE_SCHEMA_NAME": "SCHEMA_TECHM_RDR.json",
    "BUCKET_NAME": "nissan-nna-customer360-dev-snflkdev-inbound-us-east-1/C360_Datalake",
    "SCHEMA_BUCKET_NAME": "customer360-dev-gluejob-source-code",
    "CLEAN_SNOWPIPE": "SDDBC360.C360_DATA_HS.PIPE_CNI_RDR_HS",
    "ERROR_SNOWPIPE": "SDDBC360.C360_DATA_HS.PIPE_SNOWPIPE_INGST_ERR",
    "DERIVED_LOAD_PROCEDURE": "DERIVED_LOAD_PROCEDURE",
    "RUN_ID": "20e134a2-3de7-4c45-8286-b1cecfc01f27",
    "HEADERS": "False",
    "FOOTER": "False"
}

# ## @Job params
# glue_client = boto3.client("glue", region_name="us-east-1")
# args = getResolvedOptions(sys.argv, ['JOB_NAME', 'WORKFLOW_NAME', 'WORKFLOW_RUN_ID', 'EVENT_BUS_NAME'])
WORKFLOW_NAME = args['WORKFLOW_NAME']
WORKFLOW_RUN_ID = args['WORKFLOW_RUN_ID']
EVENT_BUS = args['EVENT_BUS_NAME']
#WORKFLOW_PARAMS = glue_client.get_workflow_run_properties(Name=WORKFLOW_NAME, RunId=WORKFLOW_RUN_ID)["RunProperties"]

REGION = WORKFLOW_PARAMS['REGION']
VENDOR = WORKFLOW_PARAMS['VENDOR']
RUN_ID = WORKFLOW_PARAMS['RUN_ID']
HEADERS = WORKFLOW_PARAMS['HEADERS']
FOOTER = WORKFLOW_PARAMS['FOOTER']
FILE_ROW_LENGTH = WORKFLOW_PARAMS['FILE_ROW_LENGTH']
FILE_NAME = WORKFLOW_PARAMS['FILE_NAME']
FILE_SCHEMA_NAME = WORKFLOW_PARAMS['FILE_SCHEMA_NAME']
BUCKET_NAME = WORKFLOW_PARAMS['BUCKET_NAME']
SCHEMA_BUCKET_NAME = WORKFLOW_PARAMS['SCHEMA_BUCKET_NAME']
CLEAN_SNOWPIPE = WORKFLOW_PARAMS['CLEAN_SNOWPIPE']
ERROR_SNOWPIPE = WORKFLOW_PARAMS['ERROR_SNOWPIPE']
DERIVED_LOAD_PROCEDURE = WORKFLOW_PARAMS['DERIVED_LOAD_PROCEDURE']

BUCKET = BUCKET_NAME.split("/")[0]
BUCKET_SUFFIX = BUCKET_NAME.split("/")[1]

ERROR_FILE_NAME_TMP = '.'.join(FILE_NAME.split(".")[:-1])
ERROR_FILE_NAME = ''
if '.'.join(ERROR_FILE_NAME_TMP.split(".")[:-1]) != '':
    ERROR_FILE_NAME = '.'.join(ERROR_FILE_NAME_TMP.split(".")[:-1]) + "_{}.json"
else:
    ERROR_FILE_NAME = ERROR_FILE_NAME_TMP + "_{}.json"

TARGET_FILE_NAME_TMP = '.'.join(FILE_NAME.split(".")[:-1])
TARGET_FILE_NAME = ''
if '.'.join(TARGET_FILE_NAME_TMP.split(".")[:-1]) != '':
    TARGET_FILE_NAME = '.'.join(TARGET_FILE_NAME_TMP.split(".")[:-1]) + ".csv"
else:
    TARGET_FILE_NAME = TARGET_FILE_NAME_TMP + ".csv"

FILE_SCHEMA_URI = "schema/"+VENDOR+"/"+FILE_SCHEMA_NAME
FILE_SOURCE_URI = "C360_TEST_DATA/"+VENDOR+"/"+FILE_NAME
FILE_TARGET_URI = "C360_TEST_DATA/"+VENDOR+"/output/"+TARGET_FILE_NAME
ERROR_FILE_TARGET_URI = "C360_TEST_DATA/"+VENDOR+"/error/{}"

COMPONENT_NAME = "c360-fixed-length-job"
EVENT_START_TIME = str((time.time_ns()))
UUID_STR = uuid.uuid4().hex

# FILE_SCHEMA_URI = "s3://customer360-dev-gluejob-source-code/schema/TECHM/SCHEMA_TECHM_RDR.json"
# FILE_SOURCE_URI = "s3://ns-nissan-nna-customer360-dev-snflkdev-inbound-us-east-1/C360_Datalake/SNOWFLAKE-RAW/TECHM/Daily_New_RDR_File_20220113.txt"
# FILE_TARGET_URI = "s3://ns-nissan-nna-customer360-dev-snflkdev-inbound-us-east-1/C360_Datalake/SNOWFLAKE-CLEAN/TECHM/Daily_New_RDR_File_20220113.csv"

IDQ_URL = "https://data-dq-informaticadg.na.nissancloud.com/DataIntegrationService/WebService/addressValidator_service_en/"
ADDRESS_TAGS = ['tns:address_line_1', 'tns:address_line_2', 'tns:address_po_box', 'tns:address_sub_type',
                'tns:address_sub_value', 'tns:address_city', 'tns:address_state', 'tns:address_zipcode',
                'tns:address_zip4', 'tns:address_zipcode_ext', 'tns:address_country',
                'tns:address_country_iso2']

print('RunId: ', RUN_ID)
print('Vendor: ', VENDOR)
print('File Row Length: ', FILE_ROW_LENGTH)
print('File Name: ', FILE_NAME)
print('Schema Name: ', FILE_SCHEMA_NAME)
print('Bucket Name: ', BUCKET_NAME)
print('Error Snowpipe: ', ERROR_SNOWPIPE)
print('File Schema URI: ', FILE_SCHEMA_URI)
print('File Source URI: ', FILE_SOURCE_URI)
print('File Target URI: ', "s3://" + BUCKET + "/" + FILE_TARGET_URI)

## Validation Constants specific to file
ERROR_COLUMN = "isErrorRow"

## Initializing Spark and Glue Contexts
spark = SparkSession.builder.appName("c360-fixed-length-job").master("local[2]").getOrCreate()
print("Context's initiated. Starting Job '{}'".format(WORKFLOW_NAME))

# Need to do ARN Configuration
event_client = boto3.client('events', region_name=REGION)
print('Event Client initiated')


def logEvent(event_type, event_status, event_status_text, run_id, file_name, end_time, valid_row_cnt, err_row_cnt):
    event_client = boto3.client('events', region_name=REGION)
    # eventBus = str(os.environ["EVENT_BUS_NAME"])

    eventResponse = event_client.put_events(
        Entries=[
            {
                'Source': 'glue-fixed-length-job',
                'DetailType': 'event-log',
                'Detail': json.dumps({
                    "logType": event_type,
                    "processId": UUID_STR,
                    "runId": run_id,
                    "fileName": file_name,
                    "componentStatus": event_status,
                    "componentStatusText": event_status_text,
                    "componentName": COMPONENT_NAME,
                    "componentStartTime": EVENT_START_TIME,
                    "logUpdatedTime": end_time,
                    "componentRowsProcessed": valid_row_cnt,
                    "componentRowsErrored": err_row_cnt
                }),
                'EventBusName': EVENT_BUS
            },
        ]
    )
    print(eventResponse)

# Log Event
logEvent("OPS", "STARTED", "Starting glue job", RUN_ID, FILE_NAME, time.time_ns(),0,0)

#################################### Step 0 - Reading file from S3  ####################################
# Reading fixed length file from S3

# Validating Header and Footer
df = spark.read.text(FILE_SOURCE_URI)
df_idx = df.withColumn("index", monotonically_increasing_id())
if HEADERS.lower() == "true":
    header = df_idx.first()[0]
    hd = "True"
    ft = "False"
    df = df.filter(~col("value").contains(header))
    if FOOTER.lower() == "true":
        footer = df_idx.orderBy(desc("index")).first()[0]
        ft = "True"
        df = df.filter(~col("value").contains(footer))
    print("HEADERS == {} and FOOTER == {}".format(hd, ft))
    print("Record Count:", df.count())
else:
    hd = "False"
    ft = "False"
    if FOOTER.lower() == "true":
        footer = df_idx.orderBy(desc("index")).first()[0]
        ft = "True"
        df = df.filter(~col("value").contains(footer))
    print("HEADERS == {} and FOOTER == {}".format(hd, ft))
    print("Record Count:", df.count())

#################################### Step 1 - Validation Processing ####################################

# Setting values based on conditions to new column
df = df.withColumn(ERROR_COLUMN, when(length(df.value) != FILE_ROW_LENGTH, "True").otherwise("False"))

# Filtering valid, invalid rows to respective df's
df_valid = df.filter(df.isErrorRow == "False").drop("isErrorRow")
df_invalid = df.filter(df.isErrorRow == "True").drop("isErrorRow")
df_valid.show()
print("Valid Records:"+str(df_valid.count()))
print("Invalid Records:"+str(df_invalid.count()))

# Terminating job if any invalid row is available
if df_invalid.count() != 0:
    df_invalid.show()
    print("Rows length mismatch, required row count:" + str(FILE_ROW_LENGTH))
    print("System Terminated and Service Now Ticket is raised")
    ## TODO
    ## Log event
    logEvent("OPS", "ERROR", "Row Count mismatch", RUN_ID, FILE_NAME, time.time_ns(),0,0)

    ## logic to send a ticket to service now
    sys.exit(-1)

# Schema formation for Fixed Length file type
schema = spark.read.format("json").option("header", "true").option("multiline", "true").json(FILE_SCHEMA_URI)
sDict = map(lambda x: x.asDict(), filter(lambda row: ((row['column'] != 'Filler')), schema.collect()))

# TODO: Length Check
df_validated = df_valid.select(
    *[substring(str='value', pos=int(row['from']), len=int(row['length'])).alias(str(row['column']))
      for row in sDict])
print("Mastered provided raw data")
# df_validated.show()

try:
    # Removing whitespaces from column
    for colname in df_validated.columns:
        df_validated = df_validated.withColumn(colname, trim(col(colname)))

    # Adding None/Null to empty space values
    df_validated = df_validated.select([when(col(c) == "", None).otherwise(col(c)).alias(c) for c in df_validated.columns])
    print("Removed whitespace characters and added None/Null")

except Exception as e:
    print("Error while dataframe cleanup. Key not found/ Dot value in column name/ Schema and Dataframe Column Mismatch")
    raise Exception("Error while dataframe cleanup. Key not found/ Dot value in column name/ Schema and Dataframe Column Mismatch : ", e)

df_validated.show()

#################################### Step 2 - Data-quality Check ####################################
# Extracting actual column names and forming violation column names
df_columns = df_validated.schema.names
vio_columns = list(map(lambda x: "VIO_" + x, df_validated.columns))

# Regex Dictionary
regexDict = {
    "NAME": "^[a-zA-Z][a-zA-Z\\. ]*$",
    "VIN": "(?=.*\\d|=.*[A-Z])(?=.*[A-Z])[A-Z0-9]",
    "SPL_VIN" : "(?=.*\\d|=.*[A-Z])(?=.*[A-Z])[A-Z0-9]{17}",
    "ALPHA_NUM": "^[A-Za-z0-9_@]+$",
    "CHAR": "^[A-Za-z ]+$",
    "NUMBER": "^[0-9]+$",
    # "NUMBER_2": "[+-]?[0-9]+\\.[0-9]+$", # TODO: To be checked
    "NUMBER_2": "^(?=.)([+-]?([0-9]*)(\\.([0-9]+))?)", # Fixed
    # "EMAIL": "(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$)",
    "EMAIL": "^(?=[\\w\\s+-.])\\s*[-+.'\\w]+@[-.\\w]+\\.[-.\\w]+\\s*$",
    "PHONE": "^[0-9-+]+$",
    "TEXT": "^[ A-Za-z0-9_@.:/#&'+-]+$",
    "MONEY": "[0-9]+(\\.[0-9]+)?",
    "DATE_YYYYMMDD": "((19|20[0-9][0-9])([0][0-9]|[1][0-2])([0-2][0-9]|3[0-1]))$",
    "DATE_MMDDYYYY": "(([0][0-9]|[1][0-2])([0-2][0-9]|3[0-1])(19|20[0-9][0-9]))$",
    "DATE_YYYYMM": "^((\d{4})(0?[1-9]|1[012]))$",
    "DATE_YYYY": "^[0-9]{1,4}$",
    "DATE_MMDDYYYY_2": "((0?[1-9]|1[012])/(0?[1-9]|[12][0-9]|3[01])/(\\d{4}))$",
    "DATE_YYYYMMDD_2": "((\\d{4})\\-(0?[1-9]|1[012])\\-(0?[1-9]|[12][0-9]|3[01]))$",
    "DATE_YY": "^[0-9]{1,2}$",
    "DATE_YYMMDD": "^\\d{2}(0?[1-9]|[12][0-9]|3[01])(0?[1-9]|1[012])$",
    "DATE_YYMMDD_2": "^\\d{2}(0?[1-9]|[12][0-9]|3[01])(0?[1-9]|1[012])\\.[0-9]$",
    "DATE_YYYY_1": "^[0-9]{4}\\.[0-9]",
    "TS_1": "",
    "TS_2": "",
    "TS_3": "",
    "TS_4": "",
    "TS_5": "",
    "TS_6": "",
    "BOOLEAN": "",
    "ADDRESS": "",
    "WILDCARD": ""
}

print("Data Quality Check Started")

# Prepare dictionary from Json for Rule based processing

# Get PK / Composite Key headers from schema file, for Non-essential PK "dataType":"WILDCARD" and "regex":"True"
schemaDict_nullable_f_regex_t = map(lambda x: x.asDict(),filter(lambda row: (row['nullable'] == 'False') & (row['regex'] == 'True'),schema.collect()))

# Get core / essential headers [ Name,Email,Phone,Address,VIN ] from schema file
schemaDict_nullable_t_regex_t = map(lambda x: x.asDict(),filter(lambda row: (row['nullable'] == 'True') & (row['regex'] == 'True'),schema.collect()))

# Get non-core / non- essential headers from schema file
schemaDict_nullable_t_regex_f = map(lambda x: x.asDict(),filter(lambda row: (row['nullable'] == 'True') & (row['regex'] == 'False'),schema.collect()))

# Default case for invalid configuration { To avoid missing any remaining columns }
schemaDict_nullable_f_regex_f = map(lambda x: x.asDict(),filter(lambda row: (row['nullable'] == 'False') & (row['regex'] == 'False'),schema.collect()))

## Processing Records as per rules and adding score(0 or 1) to new column with prefix VIO_* {2}
df_pre_cleansed = spark.range(0).drop("X")

try:
    df_pre_cleansed = df_validated.select(
        *df_columns,
        *[
            (when(df_validated[row["column"]].isNull() | ~df_validated[row["column"]].rlike(str(regexDict[row["dataType"]]) + str("{" + str(row["length"]) + "}$")), 1).otherwise(0)).alias('VIO_' + row['column'])
            for row in schemaDict_nullable_f_regex_t
        ],
        *[
            (when(~df_validated[row["column"]].rlike(str(regexDict[row["dataType"]]) + str("{" + str(row["length"]) + "}$")), None).otherwise(0)).alias('VIO_' + row['column'])
            for row in schemaDict_nullable_t_regex_t
        ],
        *[
            (lit(0)).alias('VIO_' + row['column'])
            for row in schemaDict_nullable_t_regex_f
        ],
        *[
            (when(df_validated[row["column"]].isNull(), 1).otherwise(0)).alias('VIO_' + row['column'])
            for row in schemaDict_nullable_f_regex_f
        ]
    )
except Exception as e:
    print("Schema and Dataframe Column Mismatch")
    raise Exception("Schema and Dataframe Column Mismatch : ", e)


# df_pre_cleansed.show()
print("Data Quality Check Finished")

df_post_cleansed = df_pre_cleansed

#################################### Step 2 - Cleansing Processing ####################################
# TODO: Merge all the cleansing codes

def _update_dataframe(related_columns=None, col_name=None, concat_key=None):
    if related_columns:
        if isinstance(related_columns, str):
            return df_post_cleansed.select('*', concat_ws(concat_key, related_columns).alias(col_name))
        else:
            return df_post_cleansed.select('*', concat_ws(concat_key, *related_columns).alias(col_name))


###################### ADDRESS-CLEANSING #####################
sDict_address = map(lambda x: x.asDict(), filter(lambda row: (row['dataType'] == 'ADDRESS'), schema.collect()))

validAddress = udf(lambda z: _validate_address(z), StringType())
spark.udf.register("validAddress", validAddress)

uncleansedAddress = udf(lambda y: _gen_json_payload(y), StringType())
spark.udf.register("uncleansedAddress", uncleansedAddress)


def _clean_address(xml_data, _tags=ADDRESS_TAGS):
    _data = minidom.parseString(xml_data)
    _address_data = dict()
    for _tag in _tags:
        _tag_value = _data.getElementsByTagName(_tag)[0].firstChild
        _tag_value = _tag_value.nodeValue if _tag_value else ""
        _address_data.update({_tag.replace("tns:", ""): _tag_value})
    return _address_data


def _verify_address_with_idq(_payload):
    try:
        _idq_response = req.post(IDQ_URL, data=_payload, headers={'Content-Type': 'application/xml'})
        if _idq_response.status_code == 200:
            return _idq_response.text
        else:
            print("Error in IDQ address validation. The response code is {}".format(_idq_response.status_code))
            return 0
    except Exception as e:
        print("Exception in accessing the API endpoint because : {}".format(e))
        return 0


def _gen_json_payload(data):
    _json_payload = dict()
    _json_payload.update({
        "address_line_1": data[0],
        "address_line_2": data[1],
        "city_name": data[2],
        "state": data[3],
        "zip": data[4],
        "country": ""
    })
    return json.dumps(_json_payload)


def _gen_payload(data):
    _payload = """
    <soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:tns="http://www.informatica.com/dis/ws/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <soap:Body>
            <tns:mplt_addressValidator_xml_en_Operation>
                <tns:Group>
                    <tns:address_line_1>{0}</tns:address_line_1>
                    <tns:address_line_2>{1}</tns:address_line_2>
                    <tns:city_name>{2}</tns:city_name>
                    <tns:state>{3}</tns:state>
                    <tns:country></tns:country>
                    <tns:zip>{4}</tns:zip>
                </tns:Group>
            </tns:mplt_addressValidator_xml_en_Operation>
        </soap:Body>
    </soap:Envelope>""".format(data[0], data[1], data[2], data[3], data[4])
    return _payload


def _validate_address(_address_data):
    _payload = _gen_payload(_address_data)
    _verified_address = _verify_address_with_idq(_payload)
    _cleansed_address = _clean_address(_verified_address) if _verified_address else {}
    return json.dumps(_cleansed_address)


for row in sDict_address:
    if row.get('relatedColumns'):
        df_post_cleansed = df_post_cleansed.withColumn("", lit(""))
        df_post_cleansed = df_post_cleansed.withColumn("UNCLEANSED_{}".format(row['column']), uncleansedAddress(struct(row.get('relatedColumns')))).withColumn("CLEANSED_{}".format(row['column']), validAddress(struct(row.get('relatedColumns'))))

# df_post_cleansed.show(truncate=False)
print("Cleansed address data")

# Setting null values on actual DF columns using VIO null's
vio_columns_2 = df_post_cleansed.select(*vio_columns)
for column in vio_columns_2.columns:
    nonVioCol = column.replace('VIO_', '')
    df_post_cleansed = df_post_cleansed.withColumn(nonVioCol, when(vio_columns_2[column].isNull(), None).otherwise(df_post_cleansed[nonVioCol]))

# df_post_cleansed.show()

# Setting null values of violation columns to 0
df_post_cleansed = df_post_cleansed.na.fill(value=0, subset=vio_columns)
# df_post_cleansed.show()

print("Counting total Violations for each record")
df_post_cleaned = df_post_cleansed.withColumn('VIOLATION_COUNT', sum(df_post_cleansed[col].cast('int') for col in vio_columns))
# df_post_cleaned.show()

df_post_cleaned = df_post_cleaned.drop("Filler").drop("VIO_Filler").drop("")

# Dataframe having error records
df_violations = df_post_cleaned.where('VIOLATION_COUNT > 0')
ttl_err_cnt = df_violations.agg({'VIOLATION_COUNT': 'sum'}).collect()[0][0]
df_violations.show(truncate=False)
print("Total Error Count: ", ttl_err_cnt)
err_row_cnt = df_violations.count()
print("Error row Count: ", str(err_row_cnt))

df_data = df_post_cleaned.where('VIOLATION_COUNT == 0')
df_data = df_data.drop(*vio_columns).drop("VIOLATION_COUNT")
df_data.show(truncate=False)
valid_row_cnt = df_data.count()
print("Valid Row Count: " + str(valid_row_cnt))

#################################### Step 3 - Writing file to S3  ####################################
print("Writing processed dataset to S3")
s3_resource = boto3.resource('s3')

# Writing Error json files to S3
err_file_list = []
err_file_counter = 0
if err_row_cnt > 0:
    pandas_error_df = df_violations.toPandas()
    for json_dict in pandas_error_df.to_dict(orient='records'):
        json_object = json.dumps(json_dict, indent=4).replace("\\\"", "\"").replace("\"{", "{").replace("}\"", "}")
        err_file_counter += 1
        file_name = ERROR_FILE_NAME.format(err_file_counter)
        err_file_list.append(file_name)
        # s3_resource.Object(BUCKET, ERROR_FILE_TARGET_URI.format(file_name)).put(Body=json_object)


# # Writing Cleansed csv file to S3
pandas_df = df_data.toPandas()
csv_buffer = StringIO()
pandas_df.to_csv(csv_buffer, index=False, sep='|')
# s3_resource.Object(BUCKET, FILE_TARGET_URI).put(Body=csv_buffer.getvalue().replace("\"\"","\"").replace("\"{", "{").replace("}\"", "}"))

#################################### Step 4 - Trigger Error Record ingestion  ####################################
print("Triggering Error Record ingestion")
if err_row_cnt > 0:
    detail_json_error = json.dumps({
        "errorSnowpipe": ERROR_SNOWPIPE,
        "vendorName": VENDOR,
        "pickupBucketName": BUCKET_NAME,
        "errorIngestion": "TRUE",
        "logTrackerId": RUN_ID,
        "snowpipeFiles": err_file_list}
    )
    print("Detail: ", detail_json_error)

    eventResponse = event_client.put_events(
        Entries=[
            {'Source': 'glue-fixed-length-job',
             'DetailType': 'event-snowpipe-error',
             'Detail': detail_json_error,
             'EventBusName': EVENT_BUS
             },
        ]
    )
    print(eventResponse)

#################################### Step 5 - Trigger CLean zone ingestion  ####################################
print("Triggering Clean zone ingestion")
detail_json_clean = json.dumps({
    "cleanSnowpipe": CLEAN_SNOWPIPE,
    "vendorName": VENDOR,
    "pickupBucketName": BUCKET_NAME,
    "cleanIngestion": "TRUE",
    "derivedLoadProcedure": DERIVED_LOAD_PROCEDURE,
    "logTrackerId": RUN_ID,
    "snowpipeFiles": [TARGET_FILE_NAME]}
)

print("Detail: ", detail_json_clean)

eventResponse = event_client.put_events(
        Entries=[
            {'Source': 'glue-fixed-length-job',
             'DetailType': 'event-snowpipe-clean',
             'Detail': detail_json_clean,
             'EventBusName': EVENT_BUS
             },
        ]
    )

print(eventResponse)

# Log event
print("Sending Job Completed Log")
logEvent("OPS", "COMPLETED", "Process Completed", RUN_ID, FILE_NAME, time.time_ns(), valid_row_cnt, err_row_cnt )
print("Job Completed")
# job.commit()