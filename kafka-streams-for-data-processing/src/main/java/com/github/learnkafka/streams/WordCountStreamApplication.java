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
import java.util.Properties;

import static com.github.learnkafka.streams.StreamRunner.startStream;
import static com.github.learnkafka.streams.StreamsProperties.createStreamConfiguration;

public class WordCountStreamApplication implements StreamApplication{
    public static final String WORD_COUNT_INPUT = "word-count-input";
    public static final String WORD_COUNT_OUTPUT = "word-count-output";

    public static void main(String[] args) {
        startStream(new WordCountStreamApplication());
    }

    @Override
    public Properties createConfiguration() {
        return createStreamConfiguration("word-counts", "localhost:9092", "earliest");
    }

    @Override
    public Topology createTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> textLines = streamsBuilder.stream(WORD_COUNT_INPUT);
        KTable<String, Long> wordCounts = textLines.flatMapValues(WordCountStreamApplication::splitOnWhiteSpace)
                .groupBy((key, word) -> word)
                .count(Materialized.as("Counts"));
        wordCounts.toStream().to(WORD_COUNT_OUTPUT, Produced.with(Serdes.String(), Serdes.Long()));

        return streamsBuilder.build();
    }

    private static List<String> splitOnWhiteSpace(String line) {
        return Arrays.asList(line.toLowerCase().split(" "));
    }
}
