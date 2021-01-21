package com.github.learnkafka.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class StreamRunner {
    public static final boolean DONT_CLEAN_UP_STREAMS = false;
    public static final boolean CLEAN_UP_STREAMS = true;
    private KafkaStreams kafkaStreams;

    public static void startStream(KafkaStreamsParameters parameters) {
        startStream(parameters, DONT_CLEAN_UP_STREAMS);
    }

    public static void startCleanStream(KafkaStreamsParameters parameters) {
        startStream(parameters, CLEAN_UP_STREAMS);
    }

    private static void startStream(KafkaStreamsParameters parameters, boolean cleanUpStreams) {
        Properties configuration = parameters.createConfiguration();
        Topology topology = parameters.createTopology();
        StreamRunner streamRunner = new StreamRunner();
        streamRunner.kafkaStreams = new KafkaStreams(topology, configuration);

        if(cleanUpStreams) {
            streamRunner.kafkaStreams.cleanUp();
        }

        streamRunner.kafkaStreams.start();
        streamRunner.printTopology();
        streamRunner.shutdown();
    }

    private void printTopology() {
        System.out.println(kafkaStreams.toString());
    }

    private void shutdown() {
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
