import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.functions import (
    col, from_json, from_unixtime, sum, window
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

spark_host = f"spark://{spark_hostname}:{spark_port}"

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"

sparkcontext = pyspark.SparkContext.getOrCreate(
    conf=(pyspark.SparkConf().setAppName("DibimbingStreaming").setMaster(spark_host))
)
sparkcontext.setLogLevel("WARN")
spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)

schema = StructType(
    [
        StructField('order_id', StringType(), True),
        StructField('customer_id', IntegerType(), True),
        StructField('furniture', StringType(), True),
        StructField('color', StringType(), True),
        StructField('price', IntegerType(), True),
        StructField('ts', StringType(), True),
    ]
)

parsed_df = (
    stream_df.selectExpr('CAST(value AS STRING)')
    .withColumn('value', from_json(col('value'), schema))
    .select(col('value.*'))
    .withColumn('ts', from_unixtime(col('ts')).cast(TimestampType()))
)

windowed_df = (
    parsed_df.withWatermark('ts', '1 day')
    .groupBy(window('ts', '1 day').alias('timestamp'))
    .agg(sum('price').alias('running_total'))
)

def transform_batch(df):

    running_total = df.selectExpr('timestamp', 'running_total')

    data_list = running_total.collect()

    return data_list

query = (
    windowed_df.writeStream
    .foreachBatch(transform_batch)
    .format('console')
    .trigger(processingTime='2 minutes')
    .outputMode('update')
    .option('checkpointLocation', '/scripts/logs')
    .start()
)

query.awaitTermination()
