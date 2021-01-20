package com.github.learnkafka.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;

import static com.github.learnkafka.streams.WordCountStreamApplication.WORD_COUNT_INPUT;
import static com.github.learnkafka.streams.WordCountStreamApplication.WORD_COUNT_OUTPUT;

public class WordCountStreamApplicationTest extends AbstractStreamApplicationTest{
    @Override
    public String getOutputTopic() {
        return WORD_COUNT_OUTPUT;
    }

    @Override
    protected String getInputTopic() {
        return WORD_COUNT_INPUT;
    }

    @Override
    public StreamApplication createStreamingApplication() {
        return new WordCountStreamApplication();
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

    private ProducerRecord<String, Long> readOutput() {
        return this.testDriver.readOutput(WORD_COUNT_OUTPUT, new StringDeserializer(), new LongDeserializer());
    }

    @Test
    void lowercaseWords(){
        sendMessage("KAFKA Kafka kafka");
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 2L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 3L);
    }
}