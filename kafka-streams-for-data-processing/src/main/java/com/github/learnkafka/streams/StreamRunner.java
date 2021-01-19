package com.github.learnkafka.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class StreamRunner {
    private KafkaStreams kafkaStreams;

    public static StreamRunner startStreamApplication(Properties config, Topology topology) {
        StreamRunner streamRunner = new StreamRunner();
        streamRunner.kafkaStreams = new KafkaStreams(topology, config);
        streamRunner.kafkaStreams.start();

        return streamRunner;
    }

    public void println() {
        System.out.println(kafkaStreams.toString());
    }

    public void shutdown() {
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
