package com.github.learnkafka.streams;

import org.apache.kafka.common.serialization.Deserializer;

public class StreamTestConfiguration<K,V> {
    private final KafkaStreamsParameters streamsParameters;
    private final Deserializer<K> keySerializer;
    private final Deserializer<V> valueSerializer;

    public StreamTestConfiguration(KafkaStreamsParameters streamsParameters, Deserializer<K> keySerializer, Deserializer<V> valueSerializer) {
        this.streamsParameters = streamsParameters;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }


    public KafkaStreamsParameters getStreamsParameters() {
        return streamsParameters;
    }

    public Deserializer<K> getKeySerializer() {
        return keySerializer;
    }

    public Deserializer<V> getValueSerializer() {
        return valueSerializer;
    }
}
