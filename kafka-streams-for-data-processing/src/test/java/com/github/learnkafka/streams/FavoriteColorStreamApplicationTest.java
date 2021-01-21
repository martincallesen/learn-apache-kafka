package com.github.learnkafka.streams;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import static com.github.learnkafka.streams.FavoriteColorStreamApplication.FAVOURITE_COLOR_INPUT;
import static com.github.learnkafka.streams.FavoriteColorStreamApplication.FAVOURITE_COLOR_OUTPUT;

public class FavoriteColorStreamApplicationTest extends AbstractKafkaStreamTest<String, Long> {
    @Override
    public StreamTestConfiguration<String, Long> testConfiguration() {
        return new StreamTestConfigurationBuilder<String, Long>()
                .streamsParameters(new FavoriteColorStreamApplication())
                .keySerializer(new StringDeserializer())
                .valueSerializer(new LongDeserializer())
                .build();
    }

    @Test
    public void favoriteColorsWithUpdateOnBlue() {
        writeInput(FAVOURITE_COLOR_INPUT, "stephane,blue");
        assertOutput(FAVOURITE_COLOR_OUTPUT, "blue", 1L);
        writeInput(FAVOURITE_COLOR_INPUT, "john,green");
        assertOutput(FAVOURITE_COLOR_OUTPUT, "green", 1L);
        writeInput(FAVOURITE_COLOR_INPUT, "stephane,red");
        assertOutput(FAVOURITE_COLOR_OUTPUT, "blue", 0L);
        assertOutput(FAVOURITE_COLOR_OUTPUT, "red", 1L);
        writeInput(FAVOURITE_COLOR_INPUT, "alice,red");
        assertOutput(FAVOURITE_COLOR_OUTPUT, "red", 2L);
    }
}