package com.github.learnkafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.Random;

public class BankTransactionTestEventProducerApplication {
    public static final Logger LOGGER = LoggerFactory.getLogger(BankTransactionTestEventProducerApplication.class);

    public static void main(String[] args) {
        LOGGER.info("BankTransactionTestEventProducerApplication is running");
        String topic = "bank-transactions-input";
        Properties configuration = createProducerConfiguration("127.0.0.1:9092");
        BankTransactionTestEventProducerApplication application = new BankTransactionTestEventProducerApplication();
        KafkaProducer<String, String> producer = application.createProducer(configuration);
        int batchNumber = 0;

        try {
            while (true) {
                LOGGER.info("Sending batch " + batchNumber++);
                producer.send(createRandomTransaction(topic, "John"));
                Thread.sleep(100);
                producer.send(createRandomTransaction(topic, "Hellen"));
                Thread.sleep(100);
                producer.send(createRandomTransaction(topic, "Christian"));
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        producer.close();
    }

    public KafkaProducer<String, String> createProducer(Properties producerConfiguration) {
        return new KafkaProducer<>(producerConfiguration);
    }

    private static ProducerRecord<String, String> createRandomTransaction(String topic, final String name) {
        ObjectNode objectNode = createRandomTransaction(name);

        return new ProducerRecord<>(topic, name, objectNode.toString());
    }

    public static ObjectNode createRandomTransaction(String name) {
        int amount = getRandomNumberInRange(1, 1000);
        String time = "" + LocalDateTime.now();

        return createTransaction(name, amount, time);
    }

    public static ObjectNode createTransaction(String name, int amount, String time) {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("name", name);
        objectNode.put("amount", amount);
        objectNode.put("time", time);

        return objectNode;
    }

    private static Properties createProducerConfiguration(String bootstrapServers) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Strong producing guarantee
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");

        //Idempotent So duplicates will not be created
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

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
