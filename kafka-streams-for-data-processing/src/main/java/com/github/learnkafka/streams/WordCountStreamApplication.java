package com.github.learnkafka.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

import static com.github.learnkafka.streams.StreamRunner.startStreamApplication;
import static com.github.learnkafka.streams.StreamsProperties.createStreamConfiguration;
import static com.github.learnkafka.streams.WordCountTopologyBuilder.buildWordCountTopology;

public class WordCountStreamApplication {
    public static void main(String[] args) {
        Properties config = createStreamConfiguration("word-counts", "localhost:9092", "earliest");
        Topology topology = buildWordCountTopology("word-count-input", "word-count-output");
        KafkaStreams streams = startStreamApplication(config, topology);
        System.out.println(streams.toString());
        StreamRunner.shutdownStreamApplication(streams);
    }
}
