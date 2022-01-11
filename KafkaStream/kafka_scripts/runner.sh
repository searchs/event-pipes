#!/usr/bin/env sh


 ################################################################################

#        RUN EACH COMMAND ON SEPARATE TERMINALS

 ################################################################################

#local Kafka: /Users/screative/devbox/engineering/kafka/kafka3
local_kafka=$HOME/devbox/engineering/kafka/kafka3
# Start ZooKeeper
#sh $local_kafka/bin/zookeeper-server-start.sh $local_kafka/config/zookeeper.properties
sh /Users/screative/devbox/engineering/kafka/kafka3/bin/zookeeper-server-start.sh /Users/screative/devbox/engineering/kafka/kafka3/config/zookeeper.properties

# Start Kafka Server
#sh $local_kafka/bin/kafka-server-start.sh $local_kafka/config/server.properties
sh /Users/screative/devbox/engineering/kafka/kafka3/bin/kafka-server-start.sh /Users/screative/devbox/engineering/kafka/kafka3/config/server.properties

#  Create topic
#sh $local_kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic invoices
sh /Users/screative/devbox/engineering/kafka/kafka3/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic invoices

#Start Producer
#sh $local_kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic invoices
sh /Users/screative/devbox/engineering/kafka/kafka3/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic invoices
