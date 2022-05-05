from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import from_json, expr
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    LongType,
    DoubleType,
    IntegerType,
    ArrayType,
)

from lib.logger import Log4j

if __name__ == "__main__":
    # setup spark
    spark = (
        SparkSession.builder.appName("File Streaming app")
        .master("local[3]")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )

    logger = Log4j(spark)

    schema = StructType(
        [
            StructField("InvoiceNumber", StringType()),
            StructField("CreatedTime", LongType()),
            StructField("StoreID", StringType()),
            StructField("PosID", StringType()),
            StructField("CashierID", StringType()),
            StructField("CustomerType", StringType()),
            StructField("CustomerCardNo", StringType()),
            StructField("TotalAmount", DoubleType()),
            StructField("NumberOfItems", IntegerType()),
            StructField("PaymentMethod", StringType()),
            StructField("CGST", DoubleType()),
            StructField("SGST", DoubleType()),
            StructField("CESS", DoubleType()),
            StructField("DeliveryType", StringType()),
            StructField(
                "DeliveryAddress",
                StructType(
                    [
                        StructField("AddressLine", StringType()),
                        StructField("City", StringType()),
                        StructField("State", StringType()),
                        StructField("PinCode", StringType()),
                        StructField("ContactNumber", StringType()),
                    ]
                ),
            ),
            StructField(
                "InvoiceLineItems",
                ArrayType(
                    StructType(
                        [
                            StructField("ItemCode", StringType()),
                            StructField("ItemDescription", StringType()),
                            StructField("ItemPrice", DoubleType()),
                            StructField("ItemQty", IntegerType()),
                            StructField("TotalValue", DoubleType()),
                        ]
                    )
                ),
            ),
        ]
    )

    # subscribe to Kafka topic
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "invoices")
        .option("startingOffsets", "earliest")
        .load()
    )

    # Check the data Schema
    kafka_df.printSchema()

    # Transformation step
    value_df = kafka_df.select(
        from_json(Column("value").cast("string"), schema).alias("value")
    )

    # value_df.printSchema()

    # Explode ingested data

    explode_df = value_df.selectExpr("value.InvoiceNumber",
                                     "value.CreatedTime",
                                     "value.PosID",
                                     "value.CustomerType",
                                     "value.PaymentMethod",
                                     "value.DeliveryType",
                                     "value.DeliveryAddress.City",
                                     "value.DeliveryAddress.State",
                                     "value.DeliveryAddress.PinCode",
                                     "explode(value.InvoiceLineItems) as LineItem",
                                     )

    flattened_df = (
        explode_df.withColumn("ItemCode", expr("LineItem.ItemCode"))
            .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
            .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
            .withColumn("ItemQty", expr("LineItem.ItemQty"))
            .withColumn("TotalValue", expr("LineItem.TotalValue"))
            .drop("LineItem")
    )

    invoice_writer_query = (flattened_df.writeStream
                            .format("json")
                            .queryName("Flattened Invoice Writer 2")
                            .outputMode("append")
                            .option("path", "output")
                            .option("checkpointLocation", "chk-point-dir")
                            .trigger(processingTime="1 minute")
                            .start()
                            )

    logger.info("Listening to Kafka")
    invoice_writer_query.awaitTermination()