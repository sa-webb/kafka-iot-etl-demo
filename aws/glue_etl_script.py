import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.functions import from_unixtime, substring

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

args = getResolvedOptions(sys.argv, [
    "job",
    "db",
    "table",
    "pushdown_predicate",
    "topic",
    "year",
    "month",
    "day"
])

job_name = args["job"]
db_name = args["db"]
table_name = args["table"]
pushdown_predicate = args["pushdown_predicate"]
topic = args["topic"]
year = args["year"]
month = args["month"]
day = args["day"]

# e.g. "iot-etl-demo-2022-12-20"
job.init("{job_name}-{}-{}-{}".format(job_name,
         year[5:], month[6:], day[4:]), args)

data = glueContext.create_dynamic_frame.from_catalog(
    database=db_name,
    table_name=table_name,
    push_down_predicate=pushdown_predicate
)

# Select only the fields we need
df = data.select_fields(["timestamp", "values"]).toDF()
# Slice the timestamp to create a date column
df = df.withColumn("ts_trunc", substring(df.timestamp, 0, 10).cast("int"))
# Create a date column from the timestamp
df = df.withColumn("date", from_unixtime(df.ts_trunc))
# Create a minute index column
df = df.withColumn("minute_index", substring(df.date, 15, 2).cast("int"))
# Create a column for the current value
df = df.withColumn("current_value", df.values[df.minute_index])
# Drop the columns we don't need
df = df.drop("values", "ts_trunc", "date", "minute_index")

out = DynamicFrame.fromDF(df, glueContext, "iot-etl-demo-data")

glueContext.write_dynamic_frame.from_options(
    frame=out,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://json-iot-data/topics/",
        "partitionKeys": [topic, year, month, day]
    },
    format_options={"compression": "snappy"},
)

job.commit()
