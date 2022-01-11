from pyparsing import col
from pyspark.sql import SparkSession
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

""" 
 Algorithm for Spark Streaming
1. Read a Streaming Source - Input DataFrame
2. Transform - Output DataFrame
3. Write the output - Sink
"""


if __name__ == "__main__":

    spark = (
        SparkSession.builder.appName("Streaming Kafka Starter")
        .master("local[3]")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.shuffle.partitions", 3)
        .config("spark.sql.streaming.schemaInference", "true")
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

    print("KAFKA DF NOW READY???????????????????")

    kafka_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "invoices")
        .option("startingOffsets", "earliest")
        .load()
    )

    value_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("value")
    )

    # value_df.show()
    notification_df = value_df.select(
        "value.InvoiceNumber", "value.CustomerCardNo", "value.TotalAmount"
    ).withColumn("EarnedLoyaltyPoints", expr("TotalAmount * 0.2"))


kafka_target_df = notification_df.selectExpr(
    "InvoiceNumber as key",
    """to_json(named_struct(
                                             'CustomerCardNo', CustomerCardNo,
                                             'TotalAmount', TotalAmount,
                                             'EarnedLoyaltyPoints', TotalAmount * 0.2
                                             )) as value""",
)
# notification_df.show()
notification_writer_query = (
    kafka_target_df.writeStream.queryName("Notification Writer")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "nofications")
    .outputMode("append")
    .option("checkpointLocation", "chk-point-dir")
    .start()
)
