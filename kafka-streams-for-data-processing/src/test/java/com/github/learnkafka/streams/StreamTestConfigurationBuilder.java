package com.github.learnkafka.streams;

import org.apache.kafka.common.serialization.Deserializer;

public class StreamTestConfigurationBuilder<K, V> {
    private String input;
    private String output;
    private StreamApplication application;
    private Deserializer<K> keySerializer;
    private Deserializer<V> valueSerializer;

    public StreamTestConfigurationBuilder<K, V>  inputTopic(String input) {
        this.input = input;
        return this;
    }

    public StreamTestConfigurationBuilder<K, V>  outputTopic(String output) {
        this.output = output;
        return this;
    }

    public StreamTestConfigurationBuilder<K, V>  application(StreamApplication application) {
        this.application = application;
        return this;
    }

    public StreamTestConfigurationBuilder<K, V>  keySerializer(Deserializer<K> keySerializer) {
        this.keySerializer = keySerializer;
        return this;
    }

    public StreamTestConfigurationBuilder<K, V>  valueSerializer(Deserializer<V> valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }

    public StreamTestConfiguration<K,V> build() {
        return new StreamTestConfiguration<>(input, output, application, keySerializer, valueSerializer);
    }
}