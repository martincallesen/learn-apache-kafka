package com.github.learnkafka.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class StreamRunner {
    public static final boolean DONT_CLEAN_UP_STREAMS = false;
    public static final boolean CLEAN_UP_STREAMS = true;
    private KafkaStreams kafkaStreams;

    public static StreamRunner startStream(Properties config, Topology topology) {
        return startStream(config, topology, DONT_CLEAN_UP_STREAMS);
    }

    public static StreamRunner startStream(Properties config, Topology topology, boolean cleanUp) {
        StreamRunner streamRunner = new StreamRunner();
        streamRunner.kafkaStreams = new KafkaStreams(topology, config);

        if(cleanUp) {
            streamRunner.kafkaStreams.cleanUp();
        }

        streamRunner.kafkaStreams.start();

        return streamRunner;
    }

    public static void startStream(StreamApplication application) {
        startStream(application, DONT_CLEAN_UP_STREAMS);
    }

    public static void startCleanStream(StreamApplication application) {
        startStream(application, CLEAN_UP_STREAMS);
    }

    private static void startStream(StreamApplication application, boolean cleanUpStreams) {
        Properties configuration = application.createConfiguration();
        Topology topology = application.createTopology();
        StreamRunner streamRunner = StreamRunner.startStream(configuration, topology, cleanUpStreams);
        streamRunner.printTopology();
        streamRunner.shutdown();
    }

    public void printTopology() {
        System.out.println(kafkaStreams.toString());
    }

    public void shutdown() {
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
