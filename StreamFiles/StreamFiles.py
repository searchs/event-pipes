from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from loguru import logger

# from lib.logger import Log4j
from utils import utils

"""
 Algorithm for Spark Streaming
1. Read a Streaming Source - Input DataFrame
2. Transform - Output DataFrame
3. Write the output - Sink
"""


if __name__ == "__main__":
    spark = utils.create_spark_streaming_session_with_graceful_shutdown(
        app_name="Streaming File content only",
        master_config="local[3]",
        partition_shuffle=3,
        spark=SparkSession,
    )

    # Read step
    ingest_df = spark.readStream.format("json").option("path", "input").load()

    ingest_df.printSchema()

    # Transform step
    explode_df = ingest_df.selectExpr(
        "InvoiceNumber",
        "CreatedTime",
        "StoreID",
        "PosID",
        "CustomerType",
        "PaymentMethod",
        "DeliveryType",
        "DeliveryAddress.City",
        "DeliveryAddress.State",
        "DeliveryAddress.PinCode",
        "explode(InvoiceLineItems) as LineItem",
    )

    explode_df.printSchema()  # check Schema

    flattened_df = (
        explode_df.withColumn("ItemCode", expr("LineItem.ItemCode"))
        .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
        .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
        .withColumn("ItemQty", expr("LineItem.ItemQty"))
        .withColumn("TotalValue", expr("LineItem.TotalValue"))
        .drop("LineItem")
    )

    # flattened_df.printSchema() # check Schema
    invoice_writer_query = (
        flattened_df.writeStream.format("json")
        .option("path", "output")
        .option("checkpointLocation", "chk-point-dir")
        .outputMode("append")
        .queryName("Flattened Invoice Writer")
        .trigger(processingTime="1 minute")
        .start()
    )

    logger.info("Flattened Invoice Writer started ")
    invoice_writer_query.awaitTermination()
    logger.iinfo("Stream processing done. Shutdown.")
