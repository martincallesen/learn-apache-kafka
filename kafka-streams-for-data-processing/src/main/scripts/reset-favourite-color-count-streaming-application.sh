~/kafka/kafka_2.13-2.6.0/bin/kafka-topics.sh --bootstrap-server  localhost:9092 --delete --topic favourite-color-intermediary
~/kafka/kafka_2.13-2.6.0/bin/kafka-topics.sh --bootstrap-server  localhost:9092 --delete --topic favourite-color-output
~/kafka/kafka_2.13-2.6.0/bin/kafka-topics.sh --bootstrap-server  localhost:9092 --delete --topic favourite-color-input
~/kafka/kafka_2.13-2.6.0/bin/kafka-topics.sh --bootstrap-server  localhost:9092 --delete --topic favourite-color-favourite-color-intermediary-STATE-STORE-0000000004-changelog
~/kafka/kafka_2.13-2.6.0/bin/kafka-topics.sh --bootstrap-server  localhost:9092 --delete --topic favourite-color-FavoriteColorCounts-repartition
~/kafka/kafka_2.13-2.6.0/bin/kafka-topics.sh --bootstrap-server  localhost:9092 --delete --topic favourite-color-FavoriteColorCounts-changelog
~/kafka/kafka_2.13-2.6.0/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group favourite-color
~/kafka/kafka_2.13-2.6.0/bin/kafka-topics.sh --bootstrap-server  localhost:9092 --create --topic favourite-color-input
~/kafka/kafka_2.13-2.6.0/bin/kafka-topics.sh --bootstrap-server  localhost:9092 --create --topic favourite-color-intermediary
~/kafka/kafka_2.13-2.6.0/bin/kafka-topics.sh --bootstrap-server  localhost:9092 --create --topic favourite-color-output
