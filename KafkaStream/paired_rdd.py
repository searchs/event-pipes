from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# from utils import utils


# spark = utils.create_spark_session(app_name="Paired RDD", spark=SparkSession)

if __name__ == "__main__":
    conf = SparkConf().setAppName("Paired").setMaster("local")
    sc = SparkContext(conf=conf)

    tuples = [("Lily", 23), ("Jack", 29), ("Mary", 29), ("James", 8)]
    pairRDD = sc.parallelize(tuples)

    pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_tuple_source")

    inputStr = ["Lily 23", "Jack 29", "Mary 29", "James 8"]
    regRDD = sc.parallelize(inputStr)
    transform = regRDD.map(lambda s: (s.split(" ")[0], int(s.split(" ")[1])))
    transform.coalesce(1).saveAsTextFile("out/transformed_reg_rdd")
