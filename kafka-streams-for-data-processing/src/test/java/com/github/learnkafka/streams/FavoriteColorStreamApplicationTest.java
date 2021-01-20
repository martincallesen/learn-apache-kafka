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

import static com.github.learnkafka.streams.FavoriteColorStreamApplication.FAVOURITE_COLOR_INPUT;
import static com.github.learnkafka.streams.FavoriteColorStreamApplication.FAVOURITE_COLOR_OUTPUT;

public class FavoriteColorStreamApplicationTest {
    private TopologyTestDriver testDriver;
    private ConsumerRecordFactory<String, String> consumerRecordFactory;

    @BeforeEach
    public void createTestDriver() {
        FavoriteColorStreamApplication app = new FavoriteColorStreamApplication();
        Topology topology = app.createTopology(FAVOURITE_COLOR_INPUT, FAVOURITE_COLOR_OUTPUT);
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
    public void favoriteColorsWithUpdateOnBlue(){
        sendMessage("stephane,blue");
        OutputVerifier.compareKeyValue(readOutput(), "blue", 1L);
        sendMessage("john,green");
        OutputVerifier.compareKeyValue(readOutput(), "green", 1L);
        sendMessage("stephane,red");
        OutputVerifier.compareKeyValue(readOutput(), "blue", 0L);
        OutputVerifier.compareKeyValue(readOutput(), "red", 1L);
        sendMessage("alice,red");
        OutputVerifier.compareKeyValue(readOutput(), "red", 2L);
    }

    private ProducerRecord<String, Long> readOutput() {
        return this.testDriver.readOutput(FAVOURITE_COLOR_OUTPUT, new StringDeserializer(), new LongDeserializer());
    }

    private void sendMessage(String msg) {
        this.testDriver.pipeInput(this.consumerRecordFactory.create(FAVOURITE_COLOR_INPUT, null, msg));
    }
}