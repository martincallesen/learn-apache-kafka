package com.github.learnkafka.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

import static com.github.learnkafka.streams.StreamRunner.startStreamApplication;
import static com.github.learnkafka.streams.StreamsProperties.createStreamConfiguration;
import static com.github.learnkafka.streams.WordCountTopologyBuilder.buildWordCountTopology;

public class FavoriteColorStreamApplication {
    public static void main(String[] args) {
        Properties config = createStreamConfiguration("favourite-color-counts", "localhost:9092", "earliest");
        Topology topology = buildWordCountTopology("favourite-color-input", "favourite-color-output");
        StreamRunner streamRunner = startStreamApplication(config, topology);
        streamRunner.println();
        streamRunner.shutdown();
    }
}
