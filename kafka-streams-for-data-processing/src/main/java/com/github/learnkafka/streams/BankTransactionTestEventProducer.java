package com.github.learnkafka.streams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.Random;

public class BankTransactionTestEventProducer {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(BankTransactionTestEventProducer.class);
        logger.info("BankTransactionTestEventProducer is running");
        KafkaProducer<String, String> producer = new KafkaProducer<>(createProducerConfiguration("127.0.0.1:9092"));
        String topic = "bank-transactions-input";

        for (int i = 0; i < 100000; i++) {
            producer.send(createRandomTransaction(topic, "John"));
            producer.send(createRandomTransaction(topic, "Hellen"));
            producer.send(createRandomTransaction(topic, "Christian"));
            producer.send(createRandomTransaction(topic, "Allan"));
            producer.send(createRandomTransaction(topic, "Mette"));
            producer.send(createRandomTransaction(topic, "Jesper"));
            producer.flush();
        }

        producer.close();
    }

    private static ProducerRecord<String, String> createRandomTransaction(String topic, final String name) {
        String value = "{" +
                "\"Name\":\"" + name + "\"" +
                "\"amount\":\"" + getRandomNumberInRange(1, 1000) + "\"" +
                "\"time\":\"" + LocalDateTime.now() + "\"" +
                "}";
        return new ProducerRecord<>(topic, value);
    }

    private static Properties createProducerConfiguration(String bootstrapServers) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }

    private static int getRandomNumberInRange(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }
}
