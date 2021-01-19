package com.github.learnkafka.streams;

import org.apache.kafka.streams.Topology;

import java.util.Properties;

import static com.github.learnkafka.streams.CountTopologyBuilder.buildFavouriteColorCountTopology;
import static com.github.learnkafka.streams.StreamRunner.startStreamApplication;
import static com.github.learnkafka.streams.StreamsProperties.createStreamConfiguration;

public class FavoriteColorStreamApplication {
    public static void main(String[] args) {
        Properties config = createStreamConfiguration("favourite-color", "localhost:9092", "earliest");
        Topology topology = buildFavouriteColorCountTopology("favourite-color-input", "favourite-color-output");
        StreamRunner streamRunner = startStreamApplication(config, topology);
        streamRunner.println();
        streamRunner.shutdown();
    }
}
