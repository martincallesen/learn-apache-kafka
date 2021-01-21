package com.github.learnkafka.streams;

import org.apache.kafka.common.serialization.Deserializer;

public class StreamTestConfigurationBuilder<K, V> {
    private KafkaStreamsParameters streamsParameters;
    private Deserializer<K> keySerializer;
    private Deserializer<V> valueSerializer;

    public StreamTestConfigurationBuilder<K, V> streamsParameters(KafkaStreamsParameters streamsParameters) {
        this.streamsParameters = streamsParameters;
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
        return new StreamTestConfiguration<>(streamsParameters, keySerializer, valueSerializer);
    }
}