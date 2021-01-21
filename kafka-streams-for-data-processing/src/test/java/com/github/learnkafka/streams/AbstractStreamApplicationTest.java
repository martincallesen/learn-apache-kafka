package com.github.learnkafka.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.Properties;

public abstract class AbstractStreamApplicationTest<K, V>{
    private TopologyTestDriver testDriver;
    private ConsumerRecordFactory<String, String> consumerRecordFactory;
    private StreamTestConfiguration<K, V> testConfig;

    @BeforeEach
    public final void createTestDriver() {
        this.testConfig = testConfiguration();
        StreamApplication app = testConfiguration().getApplication();
        Topology topology = app.createTopology();
        Properties configuration = app.createConfiguration();
        this.testDriver = new TopologyTestDriver(topology, configuration);
        StringSerializer stringSerializer = new StringSerializer();
        this.consumerRecordFactory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);
    }

    public abstract StreamTestConfiguration<K,V> testConfiguration();

    @AfterEach
    public final void closeTestDriver() {
        testDriver.close();
    }

    public final void sendMessage(String msg) {
        sendMessage(null, msg);
    }

    public final void sendMessage(String key, String msg) {
        this.testDriver.pipeInput(this.consumerRecordFactory.create(testConfig.getInput(), key, msg));
    }

    public final ProducerRecord<K, V> readOutput() {
        String output = testConfig.getOutput();
        Deserializer<K> keySerializer = testConfig.getKeySerializer();
        Deserializer<V> valueSerializer = testConfig.getValueSerializer();

        return this.testDriver.readOutput(output, keySerializer, valueSerializer);
    }
}
