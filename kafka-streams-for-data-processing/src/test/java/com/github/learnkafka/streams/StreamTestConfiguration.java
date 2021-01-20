package com.github.learnkafka.streams;

import org.apache.kafka.common.serialization.Deserializer;

public class StreamTestConfiguration<K,V> {
    private final String input;
    private final String output;
    private final StreamApplication application;
    private final Deserializer<K> keySerializer;
    private final Deserializer<V> valueSerializer;

    public StreamTestConfiguration(String input, String output, StreamApplication application, Deserializer<K> keySerializer, Deserializer<V> valueSerializer) {
        this.input = input;
        this.output = output;
        this.application = application;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    public String getInput() {
        return input;
    }

    public String getOutput() {
        return output;
    }

    public StreamApplication getApplication() {
        return application;
    }

    public Deserializer<K> getKeySerializer() {
        return keySerializer;
    }

    public Deserializer<V> getValueSerializer() {
        return valueSerializer;
    }
}
