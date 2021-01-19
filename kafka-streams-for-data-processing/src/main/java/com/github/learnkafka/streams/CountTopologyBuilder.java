package com.github.learnkafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.List;

public class CountTopologyBuilder {
    public static Topology buildWordCountTopology(String inputTopic, String outputTopic) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> textLines = streamsBuilder.stream(inputTopic);
        KTable<String, Long> wordCounts = textLines.flatMapValues(CountTopologyBuilder::splitOnWhiteSpace)
                .groupBy((key, word) -> word)
                .count(Materialized.as("Counts"));
        wordCounts.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        return streamsBuilder.build();
    }

    private static List<String> splitOnWhiteSpace(String line) {
        return Arrays.asList(line.toLowerCase().split(" "));
    }

    static Topology buildFavouriteColorCountTopology(String inputTopic, String outputTopic) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> textLines = streamsBuilder.stream(inputTopic);
        KTable<String, Long> wordCounts = textLines.mapValues(CountTopologyBuilder::color)
                .groupBy((key, color) -> color)
                .count(Materialized.as("FavoriteColorCounts"));
        wordCounts.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        return streamsBuilder.build();
    }

    private static String color(String key, String value) {
        return value.split(",")[1];
    }
}
