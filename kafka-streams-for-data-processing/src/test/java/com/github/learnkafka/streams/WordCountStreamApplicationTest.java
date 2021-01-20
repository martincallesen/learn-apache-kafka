package com.github.learnkafka.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class WordCountStreamApplicationTest {
    public static final String WORD_COUNT_OUTPUT = "word-count-output";
    private TopologyTestDriver testDriver;
    private ConsumerRecordFactory<String, String> consumerRecordFactory;

    @BeforeEach
    public void createTestDriver() {
        WordCountStreamApplication app = new WordCountStreamApplication();
        Topology topology = app.createTopology("word-count-input", WORD_COUNT_OUTPUT);
        Properties configuration = app.createConfiguration();
        this.testDriver = new TopologyTestDriver(topology, configuration);
        StringSerializer stringSerializer = new StringSerializer();
        this.consumerRecordFactory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);
    }

    @AfterEach
    public void closeTestDriver() {
        testDriver.close();
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

    private void sendMessage(String msg) {
        this.testDriver.pipeInput(this.consumerRecordFactory.create("word-count-input", null, msg));
    }

    @Test
    void lowercaseWords(){
        sendMessage("KAFKA Kafka kafka");
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 2L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 3L);
    }
}