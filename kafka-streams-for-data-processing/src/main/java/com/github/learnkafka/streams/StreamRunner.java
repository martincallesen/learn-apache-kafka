package com.github.learnkafka.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class StreamRunner {
    public static final boolean DONT_CLEAN_UP_STREAMS = false;
    public static final boolean CLEAN_UP_STREAMS = true;
    private KafkaStreams kafkaStreams;

    public static StreamRunner startStreamApplication(Properties config, Topology topology) {
        return startStreamApplication(config, topology, DONT_CLEAN_UP_STREAMS);
    }

    public static StreamRunner startStreamApplication(Properties config, Topology topology, boolean cleanUp) {
        StreamRunner streamRunner = new StreamRunner();
        streamRunner.kafkaStreams = new KafkaStreams(topology, config);

        if(cleanUp) {
            streamRunner.kafkaStreams.cleanUp();
        }

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
