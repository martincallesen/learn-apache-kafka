package com.github.learnkafka.streams;

import org.apache.kafka.streams.Topology;

import java.util.Properties;

import static com.github.learnkafka.streams.CountTopologyBuilder.buildWordCountTopology;
import static com.github.learnkafka.streams.StreamsProperties.createStreamConfiguration;

public class WordCountStreamApplication {
    public static void main(String[] args) {
        Properties config = createStreamConfiguration("word-counts", "localhost:9092", "earliest");
        Topology topology = buildWordCountTopology("word-count-input", "word-count-output");
        StreamRunner streamRunner = StreamRunner.startStream(config, topology);
        streamRunner.printTopology();
        streamRunner.shutdown();
    }
}
