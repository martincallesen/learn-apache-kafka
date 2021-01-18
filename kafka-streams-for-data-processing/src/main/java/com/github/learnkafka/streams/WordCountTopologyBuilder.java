package com.github.learnkafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.List;

public class WordCountTopologyBuilder {
    public static Topology buildWordCountTopology(String inputTopic, String outputTopic) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> textLines = streamsBuilder.stream(inputTopic);
        KTable<String, Long> wordCounts = textLines.flatMapValues(WordCountTopologyBuilder::splitOnWhiteSpace)
                .groupBy((key, word) -> word)
                .count();
        wordCounts.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        return streamsBuilder.build();
    }

    private static List<String> splitOnWhiteSpace(String line) {
        return Arrays.asList(line.toLowerCase().split(" "));
    }

}
