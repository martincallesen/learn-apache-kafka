package com.github.learnkafka.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.github.learnkafka.streams.StreamRunner.*;
import static com.github.learnkafka.streams.StreamsProperties.createStreamExactlyOnceConfiguration;

public class BankBalanceStreamApplication implements KafkaStreamsParameters {
    public static final Logger LOGGER = LoggerFactory.getLogger(BankBalanceStreamApplication.class);
    public static final String BANK_TRANSACTIONS_INPUT = "bank-transactions-input";
    public static final String BANK_BALANCE_OUTPUT = "bank-balance-output";

    public static void main(String[] args) {
        startCleanStream(new BankBalanceStreamApplication());
    }

    @Override
    public Properties createConfiguration() {
        return createStreamExactlyOnceConfiguration("bank-balance", "localhost:9092", "earliest");
    }

    @Override
    public Topology createTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> inputStream = streamsBuilder.stream(BANK_TRANSACTIONS_INPUT);
        KGroupedStream<String, String> stringStringKGroupedStream = inputStream.groupByKey();
        KTable<String, String> aggregate = stringStringKGroupedStream.aggregate(init(), calculateBalance());
        aggregate.toStream().to(BANK_BALANCE_OUTPUT, Produced.with(Serdes.String(), Serdes.String()));

        return streamsBuilder.build();
    }

    private static Initializer<String> init() {
        return () -> "";
    }

    private static Aggregator<String, String, String> calculateBalance() {
        return (key, value, aggValue) -> {
            String jsonBalanceEvent = "";

            try {
                jsonBalanceEvent = createBalance(key, time(value), calculateBalance(value, aggValue));

            } catch (JsonProcessingException e) {
                LOGGER.error("Could not parsen to json", e);
            }

            return jsonBalanceEvent;
        };
    }

    private static String time(String newValue) throws JsonProcessingException {
        JsonNode jsonNode = asNode(newValue);

        return jsonNode.get("time").asText();
    }

    private static long calculateBalance(String newValue, String aggValue) throws JsonProcessingException {
        JsonNode jsonNode = asNode(newValue);
        long amount = jsonNode.get("amount").longValue();

        return amount + getBalance(aggValue);
    }

    private static long getBalance(String aggValue) throws JsonProcessingException {
        return !"".equals(aggValue) ? asNode(aggValue).get("balance").longValue() : 0L;
    }

    private static JsonNode asNode(String json) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();

        return objectMapper.readTree(json);
    }

    public static String createBalance(String key, String time, long balance) {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode node = objectMapper.createObjectNode();
        node.put("name", key);
        node.put("balance", balance);
        node.put("time", time);

        return node.toString();
    }
}
