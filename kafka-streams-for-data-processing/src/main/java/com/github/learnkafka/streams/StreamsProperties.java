package com.github.learnkafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class StreamsProperties {
    public static Properties createStreamConfiguration(String id, String server, String offsetReset) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, id);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return properties;
    }

    static Properties createStreamExactlyOnceConfiguration(String inputTopic, String outputTopic, String offset) {
        Properties streamConfiguration = createStreamConfiguration(inputTopic, outputTopic, offset);
        streamConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        return streamConfiguration;
    }
}
