package com.github.learnkafka.streams;

import org.apache.kafka.streams.Topology;

import java.util.Properties;

public interface StreamApplication {
    Topology createTopology();
    Properties createConfiguration();
}
