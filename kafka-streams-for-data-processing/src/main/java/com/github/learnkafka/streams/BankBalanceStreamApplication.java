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

import static com.github.learnkafka.streams.StreamRunner.CLEAN_UP_STREAMS;
import static com.github.learnkafka.streams.StreamRunner.startStreamApplication;

public class BankBalanceStreamApplication {
    public static final Logger LOGGER = LoggerFactory.getLogger(BankBalanceStreamApplication.class);

    public static void main(String[] args) {
        Properties config = StreamsProperties.createStreamExactlyOnceConfiguration("bank-balance", "localhost:9092", "earliest");
        Topology topology = buildBankBalanceTopology("bank-transactions-input", "bank-balance-output");
        StreamRunner streamRunner = startStreamApplication(config, topology, CLEAN_UP_STREAMS);
        streamRunner.printTopology();
        streamRunner.shutdown();
    }

    private static Topology buildBankBalanceTopology(String inputTopic, String outputTopic) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> inputStream = streamsBuilder.stream(inputTopic);
        KGroupedStream<String, String> stringStringKGroupedStream = inputStream.groupByKey();
        KTable<String, String> aggregate = stringStringKGroupedStream.aggregate(init(), calculateBalance());
        aggregate.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        return streamsBuilder.build();
    }

    private static Initializer<String> init() {
        return () -> "";
    }

    private static Aggregator<String, String, String> calculateBalance() {
        return (key, newValue, aggValue) -> {
            String jsonBalanceEvent = "";

            try {
                JsonNode jsonNode = getJsonNode(newValue);
                long amount = jsonNode.get("amount").longValue();
                String time = jsonNode.get("time").toString();
                long balance = getBalance(aggValue);
                jsonBalanceEvent = createJSONBalanceEvent(key, time, amount+balance);

            } catch (JsonProcessingException e) {
                LOGGER.error("Could not parsen to json", e);
            }

            return jsonBalanceEvent;
        };
    }

    private static long getBalance(String aggValue) throws JsonProcessingException {
        return !"".equals(aggValue) ? getJsonNode(aggValue).get("balance").longValue() : 0L;
    }

    private static JsonNode getJsonNode(String json) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();

        return objectMapper.readTree(json);
    }

    private static String createJSONBalanceEvent(String key, String time, long balance) {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode node = objectMapper.createObjectNode();
        node.put("name", key);
        node.put("balance", balance);
        node.put("time", time);

        return node.toString();
    }
}