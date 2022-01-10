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
        SparkSession.builder.appName("Streaming WordCount")
        .master("local[3]")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.shuffle.partitions", 3)
        .getOrCreate()
    )

    logger = Log4j(spark)

    # Read step
    lines_df = (
        spark.readStream.format("socket")
        .option("host", "localhost")
        .option("port", "9999")
        .load()
    )

    # lines_df.printSchema()
    # Transform step
    words_df = lines_df.select(expr("explode(split(value, ' ')) as word"))
    counts_df = words_df.groupBy("word").count()

    # Sink(Write) step
    word_count_query = (
        counts_df.writeStream.format("console")
        .option("checkpointLocaiton", "chk-point-dir")
        .outputMode("complete")
        .start()
    )

    word_count_query.awaitTermination()
