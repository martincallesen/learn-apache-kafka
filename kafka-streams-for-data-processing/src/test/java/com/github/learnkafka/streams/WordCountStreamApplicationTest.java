package com.github.learnkafka.streams;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;

import static com.github.learnkafka.streams.WordCountStreamApplication.WORD_COUNT_INPUT;
import static com.github.learnkafka.streams.WordCountStreamApplication.WORD_COUNT_OUTPUT;

public class WordCountStreamApplicationTest extends AbstractStreamApplicationTest<String, Long> {
    @Override
    public StreamTestConfiguration<String, Long> testConfiguration() {
        return new StreamTestConfigurationBuilder<String, Long>()
                .inputTopic(WORD_COUNT_INPUT)
                .outputTopic(WORD_COUNT_OUTPUT)
                .application(new WordCountStreamApplication())
                .keySerializer(new StringDeserializer())
                .valueSerializer(new LongDeserializer())
                .build();
    }

    @Test
    void multipleWords() {
        sendMessage("testing Kafka Streams");
        OutputVerifier.compareKeyValue(readOutput(), "testing", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "streams", 1L);

        sendMessage("testing Kafka again");
        OutputVerifier.compareKeyValue(readOutput(), "testing", 2L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 2L);
        OutputVerifier.compareKeyValue(readOutput(), "again", 1L);
    }

    @Test
    void lowercaseWords() {
        sendMessage("KAFKA Kafka kafka");
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 2L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 3L);
    }
}