package com.github.learnkafka.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class StreamRunner {
    public static KafkaStreams startStreamApplication(Properties config, Topology topology) {
        KafkaStreams kafkaStreams = new KafkaStreams(topology, config);
        kafkaStreams.start();

        return kafkaStreams;
    }

    public static void shutdownStreamApplication(KafkaStreams streams) {
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
