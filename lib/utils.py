import configparser

from pyspark import SparkConf
from pyspark.sql.functions import DataFrame


def load_survey_df(spark, data_file) -> DataFrame:
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(data_file)


def count_by_country(survey_df) -> DataFrame:
    """Filters data by age returning dataframe grouped by country"""
    return survey_df.filter("Age < 40") \
        .select("Age", "Gender", "Country", "state") \
        .groupBy("Country") \
        .count()


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark_conf")

    for (key,val) in config.items("SPARK_APP_CONFIGs"):
        spark_conf.set(key, val)
    return spark_conf


