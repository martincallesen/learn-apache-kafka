package com.github.learnkafka.streams;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import static com.github.learnkafka.streams.FavoriteColorStreamApplication.FAVOURITE_COLOR_INPUT;
import static com.github.learnkafka.streams.FavoriteColorStreamApplication.FAVOURITE_COLOR_OUTPUT;
import static org.apache.kafka.streams.test.OutputVerifier.compareKeyValue;

public class FavoriteColorStreamApplicationTest extends AbstractStreamApplicationTest<String, Long> {
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
        compareKeyValue(readOutput(FAVOURITE_COLOR_OUTPUT), "blue", 1L);
        writeInput(FAVOURITE_COLOR_INPUT, "john,green");
        compareKeyValue(readOutput(FAVOURITE_COLOR_OUTPUT), "green", 1L);
        writeInput(FAVOURITE_COLOR_INPUT, "stephane,red");
        compareKeyValue(readOutput(FAVOURITE_COLOR_OUTPUT), "blue", 0L);
        compareKeyValue(readOutput(FAVOURITE_COLOR_OUTPUT), "red", 1L);
        writeInput(FAVOURITE_COLOR_INPUT, "alice,red");
        compareKeyValue(readOutput(FAVOURITE_COLOR_OUTPUT), "red", 2L);
    }
}