package com.github.learnkafka.streams;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import static com.github.learnkafka.streams.BankBalanceStreamApplication.*;
import static com.github.learnkafka.streams.BankTransactionTestEventProducerApplication.createTransaction;
import static org.apache.kafka.streams.test.OutputVerifier.compareKeyValue;

public class BankBalanceStreamApplicationTest extends AbstractStreamApplicationTest<String, String>{
    @Override
    public StreamTestConfiguration<String, String> testConfiguration(){
        return new StreamTestConfigurationBuilder<String, String>()
                .inputTopic(BANK_TRANSACTIONS_INPUT)
                .outputTopic(BANK_BALANCE_OUTPUT)
                .application(new BankBalanceStreamApplication())
                .keySerializer(new StringDeserializer())
                .valueSerializer(new StringDeserializer())
                .build();
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
}