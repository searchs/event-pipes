from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

from lib.logger import Log4j

""" 
 Algorithm for Spark Streaming
1. Read a Streaming Source - Input DataFrame
2. Transform - Output DataFrame
3. Write the output - Sink
"""


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Streaming File Contents")
        .master("local[3]")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.shuffle.partitions", 3)
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )

    logger = Log4j(spark)

    # Read step
    raw_df = spark.readStream.format("json").option("path", "input").load()

    # raw_df.printSchema()

    # Transform step
    explode_df = raw_df.selectExpr(
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

    # explode_df.printSchema() # check Schema

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
    # .trigger(processingTime="1 minute") \
    # .start()

    logger.info("Flattened Invoice Writer started ")
    invoice_writer_query.awaitTermination()
