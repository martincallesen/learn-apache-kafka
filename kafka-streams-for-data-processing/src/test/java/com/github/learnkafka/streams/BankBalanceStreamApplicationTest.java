package com.github.learnkafka.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import static com.github.learnkafka.streams.BankBalanceStreamApplication.*;
import static com.github.learnkafka.streams.BankTransactionTestEventProducerApplication.createTransaction;
import static org.apache.kafka.streams.test.OutputVerifier.compareKeyValue;

public class BankBalanceStreamApplicationTest extends AbstractStreamApplicationTest{

    public String getOutputTopic() {
        return BANK_BALANCE_OUTPUT;
    }

    public String getInputTopic() {
        return BANK_TRANSACTIONS_INPUT;
    }

    public BankBalanceStreamApplication createStreamingApplication() {
        return new BankBalanceStreamApplication();
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
        return this.testDriver.readOutput(getOutputTopic(), new StringDeserializer(), new StringDeserializer());
    }
}