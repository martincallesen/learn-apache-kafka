# Description
Releated code to the udemy course https://bec.udemy.com/course/apache-kafka/learn

Installation of apache kafka
https://kafka.apache.org/quickstart

Installation of apache zookeeper
https://zookeeper.apache.org/doc/current/zookeeperStarted.html

# How to's

## Run Producer
Open a terminal and in directory ./producer
mvn compile exec:java

## Run consumer
Open a terminal and in directory ./consumer 
mvn compile exec:java

## Usefull  terminal commands

### Producer to topic with console producer
./kafka-console-producer.sh --broker-list localhost:9092 --topic topic1

### Consume from topic with console consumer
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic1 --group first-group

### List Topics
./kafka-topics.sh --bootstrap-server localhost:9092 --describe

### Create topic


### Delete topic
./kafka-topics.sh --bootstrap-server localhost:9092 --topic topic2 --delete

### See Consumer lag
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups

### Run the java producer or consumer exercise
mvn compile exec:java

### Run the java producer solution
mvn compile exec:java -Dexec-maven-plugin.mainClass=com.github.kafka.ProducerTopic1Solution

### Run the java consumer solution
mvn compile exec:java -Dexec-maven-plugin.mainClass=com.github.kafka.ConsumerTopic1Solution

### Start broker
~/kafka/kafka_2.13-2.6.0/bin/kafka-server-start.sh ~/kafka/kafka_2.13-2.6.0/config/server.properties

### Start Zookeeper
~/kafka/apache-zookeeper-3.6.2-bin/bin/zkServer.sh start


