package com.github.learnkafka.streams;

import org.apache.kafka.streams.Topology;

import java.util.Properties;

public interface KafkaStreamsParameters {
    Topology createTopology();
    Properties createConfiguration();
}
