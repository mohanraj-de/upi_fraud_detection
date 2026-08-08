from pyspark.sql import SparkSession

spark=(SparkSession.Builder().
    config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3,io.delta:delta-spark_2.12:3.2.0").
    config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType,IntegerType

schema=StructType([StructField("txn_id",StringType(),nullable=False),
                   StructField("timestamp",TimestampType(),nullable=False),
                   StructField("sender_upi",StringType(),nullable=False),
                   StructField("sender_state",StringType(),nullable=False),
                   StructField("sender_device_id",StringType(),nullable=False),
                   StructField("receiver_upi",StringType(),nullable=False),
                   StructField("receiver_type",StringType(),nullable=False),
                   StructField("receiver_category",StringType(),nullable=False),
                   StructField("amount",DoubleType(),nullable=False),
                   StructField("status",StringType(),nullable=False),
                   StructField("is_fraud",BooleanType(),nullable=True),
                   StructField("fraud_pattern",StringType(),nullable=True),])


from pyspark.sql.functions import from_json, col

# startingOffsets refers to offset from which data needs to be refered
raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "upi_transactions")
    .option("startingOffsets", "earliest")
    .load()
)

json_strings_df = raw_df.selectExpr(
    "CAST(key AS STRING) AS sender_upi",   # keep the key if you want it
    "CAST(value AS STRING) AS json_str",
    "timestamp AS kafka_timestamp"          # Kafka's own ingestion timestamp
)

# Step 2: string -> nested struct
# from_json takes a column of JSON strings + your StructType, returns
# a new column typed as `struct<...>` matching the schema. One column,
# not many yet.
parsed_df = json_strings_df.withColumn(
    "parsed", from_json(col("json_str"), schema)
)

flat_df = parsed_df.select(
    "kafka_timestamp",
    "parsed.*"
)


## add a listener in spark job to capture progress logs

from pyspark.sql.streaming import StreamingQueryListener
import logging

logging.basicConfig(
    filename="/home/jovyan/lakehouse/bronze/stream.log",
    level=logging.INFO,
    format="%(asctime)s %(message)s",
)

class RowCountListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        logging.info(f"Query started: {event.id}")

    def onQueryProgress(self, event):
        p = event.progress
        logging.info(f"batchId={p.batchId} numInputRows={p.numInputRows} rowsPerSec={p.inputRowsPerSecond}")

    def onQueryTerminated(self, event):
        logging.info(f"Query terminated: {event.id} exception={event.exception}")

    def onQueryIdle(self, event):
        pass

spark.streams.addListener(RowCountListener())

# initiate the write Stream
# checkpoint ensures spark is not reprocessing the same data. if spark stream fails mid way, the future spark job knows where to start

#query object is background process initiates by script

query = (
    flat_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/home/jovyan/lakehouse/bronze/upi_raw_checkpoint")
    .outputMode("append")
    .start("/home/jovyan/lakehouse/bronze/upi_raw")
)


# this waits until the query process terminates (manual kill)
query.awaitTermination() ## this will hold spark session till the streaming is completed