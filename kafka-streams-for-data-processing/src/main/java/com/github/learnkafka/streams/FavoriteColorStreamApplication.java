package com.github.learnkafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

import static com.github.learnkafka.streams.StreamRunner.startStream;
import static com.github.learnkafka.streams.StreamsProperties.createStreamConfiguration;

public class FavoriteColorStreamApplication implements StreamApplication{
    public static final String FAVOURITE_COLOR_INPUT = "favourite-color-input";
    public static final String FAVOURITE_COLOR_INTERMEDIARY = "favourite-color-intermediary";
    public static final String FAVOURITE_COLOR_OUTPUT = "favourite-color-output";

    public static void main(String[] args) {
        startStream(new FavoriteColorStreamApplication());
    }

    @Override
    public Properties createConfiguration() {
        return createStreamConfiguration("favourite-color", "localhost:9092", "earliest");
    }

    @Override
    public Topology createTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> inputStream = streamsBuilder.stream(FAVOURITE_COLOR_INPUT);
        inputStream.selectKey(FavoriteColorStreamApplication.keyOnName())
                .mapValues(FavoriteColorStreamApplication::colorKeyValue)
                .to(FAVOURITE_COLOR_INTERMEDIARY, Produced.with(Serdes.String(), Serdes.String()));

        KTable<String, String> table = streamsBuilder.table(FAVOURITE_COLOR_INTERMEDIARY);
        table.groupBy(FavoriteColorStreamApplication.colorKeyValue())
                .count(Materialized.as("FavoriteColorCounts"))
        .toStream().to(FAVOURITE_COLOR_OUTPUT, Produced.with(Serdes.String(), Serdes.Long()));

        return streamsBuilder.build();
    }

    private static KeyValueMapper<String, String, KeyValue<String, String>> colorKeyValue() {
        return (key, color) -> new KeyValue<>(color, "");
    }

    private static KeyValueMapper<String, String, String> keyOnName() {
        return (key, value) -> name(value);
    }

    private static String colorKeyValue(String value) {
        return value.split(",")[1];
    }

    private static String name(String value) {
        return value.split(",")[0];
    }
}
