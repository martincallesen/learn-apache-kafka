package com.github.learnkafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

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
        KStream<String, String> inputStream = streamsBuilder.stream(inputTopic);
        inputStream.selectKey(keyOnName())
                .mapValues(CountTopologyBuilder::color)
                .to("favourite-color-intermediary", Produced.with(Serdes.String(), Serdes.String()));

        KTable<String, String> table = streamsBuilder.table("favourite-color-intermediary");
        table.groupBy(color())
                .count(Materialized.as("FavoriteColorCounts"))
        .toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        return streamsBuilder.build();
    }

    private static KeyValueMapper<String, String, KeyValue<String, String>> color() {
        return (key, color) -> new KeyValue<>(color, "");
    }

    private static KeyValueMapper<String, String, String> keyOnName() {
        return (key, value) -> name(value);
    }

    private static String color(String value) {
        return value.split(",")[1];
    }

    private static String name(String value) {
        return value.split(",")[0];
    }
}
