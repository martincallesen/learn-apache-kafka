~/kafka/kafka_2.13-2.6.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic favourite-color-output \
--from-beginning \
--formatter kafka.tools.DefaultMessageFormatter \
--property print.key=true \
--property print.value=true \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer