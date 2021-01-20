package com.github.learnkafka.streams;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.Properties;

public abstract class AbstractStreamApplicationTest{
    TopologyTestDriver testDriver;
    private ConsumerRecordFactory<String, String> consumerRecordFactory;

    @BeforeEach
    public final void createTestDriver() {
        StreamApplication app = createStreamingApplication();
        Topology topology = app.createTopology(getInputTopic(), getOutputTopic());
        Properties configuration = app.createConfiguration();
        this.testDriver = new TopologyTestDriver(topology, configuration);
        StringSerializer stringSerializer = new StringSerializer();
        this.consumerRecordFactory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);
    }

    public abstract String getOutputTopic();

    protected abstract String getInputTopic();

    public abstract StreamApplication createStreamingApplication();

    @AfterEach
    public final void closeTestDriver() {
        testDriver.close();
    }


    public final void sendMessage(String msg) {
        sendMessage(null, msg);
    }

    public final void sendMessage(String key, String msg) {
        this.testDriver.pipeInput(this.consumerRecordFactory.create(getInputTopic(), key, msg));
    }
}
