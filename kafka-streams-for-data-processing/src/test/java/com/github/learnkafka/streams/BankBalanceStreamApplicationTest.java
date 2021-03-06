package com.github.learnkafka.streams;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import static com.github.learnkafka.streams.BankBalanceStreamApplication.*;
import static com.github.learnkafka.streams.BankTransactionTestEventProducerApplication.createTransaction;

public class BankBalanceStreamApplicationTest extends AbstractKafkaStreamTest<String, String> {
    @Override
    public StreamTestConfiguration<String, String> testConfiguration(){
        return new StreamTestConfigurationBuilder<String, String>()
                .streamsParameters(new BankBalanceStreamApplication())
                .keySerializer(new StringDeserializer())
                .valueSerializer(new StringDeserializer())
                .build();
    }

    @Test
    public void balanceOnTwoTransactionsOnOnePerson(){
        writeInput(BANK_TRANSACTIONS_INPUT, "John", createTransaction("John", 100, "2018-01-02").toString());
        assertOutput(BANK_BALANCE_OUTPUT, "John", createBalance("John", "2018-01-02", 100));
        writeInput(BANK_TRANSACTIONS_INPUT, "John", createTransaction("John", 300, "2018-01-02").toString());
        assertOutput(BANK_BALANCE_OUTPUT, "John", createBalance("John", "2018-01-02", 400));
    }

    @Test
    public void balanceOnTwoTransactionsOnOnePersonAndOneTransactionOnAnother(){
        writeInput(BANK_TRANSACTIONS_INPUT, "John", createTransaction("John", 100, "2018-01-02").toString());
        assertOutput(BANK_BALANCE_OUTPUT, "John", createBalance("John", "2018-01-02", 100));
        writeInput(BANK_TRANSACTIONS_INPUT, "Hellen", createTransaction("Hellen", 150, "2018-01-02").toString());
        assertOutput(BANK_BALANCE_OUTPUT, "Hellen", createBalance("Hellen", "2018-01-02", 150));
        writeInput(BANK_TRANSACTIONS_INPUT, "John", createTransaction("John", 300, "2018-01-02").toString());
        assertOutput(BANK_BALANCE_OUTPUT, "John", createBalance("John", "2018-01-02", 400));
    }
}