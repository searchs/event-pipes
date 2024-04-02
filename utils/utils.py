from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def create_spark_session(app_name: str, spark: SparkSession) -> SparkSession:
    _spark = spark.builder.appName(app_name)

    return _spark


def create_spark_streaming_session_with_graceful_shutdown(
    app_name: str, master_config: str, partition_shuffle: int, spark: SparkSession
) -> SparkSession:

    _spark = (
        spark.builder.appName(app_name)
        .master(master_config)
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.shuffle.partitions", partition_shuffle)
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )
    return _spark
