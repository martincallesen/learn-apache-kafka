package com.github.learnkafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

import static com.github.learnkafka.streams.BankBalanceStreamApplication.*;
import static com.github.learnkafka.streams.BankTransactionTestEventProducerApplication.createTransaction;
import static org.apache.kafka.streams.test.OutputVerifier.compareKeyValue;

public class BankBalanceStreamApplicationTest {
    private TopologyTestDriver testDriver;
    private ConsumerRecordFactory<String, String> consumerRecordFactory;

    @BeforeEach
    public void createTestDriver() {
        BankBalanceStreamApplication app = new BankBalanceStreamApplication();
        Topology topology = app.createTopology(BANK_TRANSACTIONS_INPUT, BANK_BALANCE_OUTPUT);
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
    public void balanceOnTwoTransactionsOnOnePerson(){
        sendMessage("John", createTransaction("John", 100, "2018-01-02").toString());
        compareKeyValue(readOutput(), "John", createBalance("John", "2018-01-02", 100));
        sendMessage("John", createTransaction("John", 300, "2018-01-02").toString());
        compareKeyValue(readOutput(), "John", createBalance("John", "2018-01-02", 400));
    }

    @Test
    public void balanceOnTwoTransactionsOnOnePersonAndOneTransactionOnAnother(){
        sendMessage("John", createTransaction("John", 100, "2018-01-02").toString());
        compareKeyValue(readOutput(), "John", createBalance("John", "2018-01-02", 100));
        sendMessage("Hellen", createTransaction("Hellen", 150, "2018-01-02").toString());
        compareKeyValue(readOutput(), "Hellen", createBalance("Hellen", "2018-01-02", 150));
        sendMessage("John", createTransaction("John", 300, "2018-01-02").toString());
        compareKeyValue(readOutput(), "John", createBalance("John", "2018-01-02", 400));
    }

    private ProducerRecord<String, String> readOutput() {
        return this.testDriver.readOutput(BANK_BALANCE_OUTPUT, new StringDeserializer(), new StringDeserializer());
    }

    private void sendMessage(String key, String msg) {
        this.testDriver.pipeInput(this.consumerRecordFactory.create(BANK_TRANSACTIONS_INPUT, key, msg));
    }
}