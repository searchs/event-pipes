# event-pipes
Apache Spark - PySpark with Kafka

#### Setup local dev env
Using Anaconda, run this command to create a virtual environment with Python 3.11
```bash
conda create -n invenv -c conda-forge python=3.11 -y
#Activate the environment
conda activate invenv
conda install -c conda-forge pyspark
conda install -c conda-forge findspark
```


### Streaming Kafka Source
Update spark-default.conf with the spark-kafka dependency
```bash
spark.jars.packages  org.apache.spark:spark-sql-kafka-0-10_2.13:3.2.0,com.google.code.findbugs:jsr305:3.0.2,org.apache.spark:spark-avro_2.13:3.2.0,net.sf.py4j:py4j:0.10.9.3 

```
