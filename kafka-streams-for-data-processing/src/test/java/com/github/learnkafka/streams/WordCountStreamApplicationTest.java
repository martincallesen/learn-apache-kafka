package com.github.learnkafka.streams;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import static com.github.learnkafka.streams.WordCountStreamApplication.WORD_COUNT_INPUT;
import static com.github.learnkafka.streams.WordCountStreamApplication.WORD_COUNT_OUTPUT;

public class WordCountStreamApplicationTest extends AbstractKafkaStreamTest<String, Long> {
    @Override
    public StreamTestConfiguration<String, Long> testConfiguration() {
        return new StreamTestConfigurationBuilder<String, Long>()
                .streamsParameters(new WordCountStreamApplication())
                .keySerializer(new StringDeserializer())
                .valueSerializer(new LongDeserializer())
                .build();
    }

    @Test
    void multipleWords() {
        writeInput(WORD_COUNT_INPUT, "testing Kafka Streams");
        assertOutput(WORD_COUNT_OUTPUT, "testing", 1L);
        assertOutput(WORD_COUNT_OUTPUT, "kafka", 1L);
        assertOutput(WORD_COUNT_OUTPUT, "streams", 1L);

        writeInput(WORD_COUNT_INPUT, "testing Kafka again");
        assertOutput(WORD_COUNT_OUTPUT, "testing", 2L);
        assertOutput(WORD_COUNT_OUTPUT, "kafka", 2L);
        assertOutput(WORD_COUNT_OUTPUT, "again", 1L);
    }

    @Test
    void lowercaseWords() {
        writeInput(WORD_COUNT_INPUT, "KAFKA Kafka kafka");
        assertOutput(WORD_COUNT_OUTPUT, "kafka", 1L);
        assertOutput(WORD_COUNT_OUTPUT, "kafka", 2L);
        assertOutput(WORD_COUNT_OUTPUT, "kafka", 3L);
    }
}