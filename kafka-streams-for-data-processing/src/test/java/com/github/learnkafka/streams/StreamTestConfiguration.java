package com.github.learnkafka.streams;

import org.apache.kafka.common.serialization.Deserializer;

public class StreamTestConfiguration<K,V> {
    private final StreamApplication application;
    private final Deserializer<K> keySerializer;
    private final Deserializer<V> valueSerializer;

    public StreamTestConfiguration(StreamApplication application, Deserializer<K> keySerializer, Deserializer<V> valueSerializer) {
        this.application = application;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
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
