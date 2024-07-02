#! /usr/bin/env sh
wget https://downloads.apache.org/kafka/3.7.0/kafka_2.12-3.7.0.tgz
tar -xzf kafka_2.12-3.7.0.tgz
cd kafka_2.12-3.7.0
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties

bin/kafka-server-start.sh config/kraft/server.properties

# Create a topic and start producer
bin/kafka-topics.sh --create --topic news --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic weather --bootstrap-server localhost:9092

# Start a producer
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic weather

# Consumer Consumer
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic weather --from-beginning

# Kraft Logs
ls /tmp/kraft-combined-logs/news-0

bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic bankbranch

# Message with keys
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic bankbranch --property parse.key=true --property key.separator=:

# Consume with keys
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bankbranch --from-beginning --property print.key=true --property key.separator=:

# Create Consumer Groups
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bankbranch --group atm-app

# Group Details
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group atm-app

# Reset Offset
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --topic bankbranch --group atm-app --reset-offsets --to-earliest --execute

# Reset Offset with Shift
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --topic bankbranch --group atm-app --reset-offsets --shift-by -2 --execute
