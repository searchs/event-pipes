import findspark
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import *

findspark.init()

if __name__ == "__main__":
    conf = get_spark_app_config()
    print("==" * 35 + "\n")
    print(conf.getAll())
    print("==" * 35 + "\n")
    spark = (
        SparkSession.builder.master("local[3]").appName("HolaSparkSql").getOrCreate()
    )

    logger = Log4j(spark)

    surveyDF = load_survey_df(spark, "data/survey.csv")
    countDF = count_by_country(surveyDF)
    countDF.show()
