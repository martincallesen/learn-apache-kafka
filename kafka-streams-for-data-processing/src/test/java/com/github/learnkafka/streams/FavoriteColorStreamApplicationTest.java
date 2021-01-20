package com.github.learnkafka.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;

import static com.github.learnkafka.streams.FavoriteColorStreamApplication.FAVOURITE_COLOR_INPUT;
import static com.github.learnkafka.streams.FavoriteColorStreamApplication.FAVOURITE_COLOR_OUTPUT;

public class FavoriteColorStreamApplicationTest extends AbstractStreamApplicationTest{
    @Override
    public String getOutputTopic() {
        return FAVOURITE_COLOR_OUTPUT;
    }

    @Override
    protected String getInputTopic() {
        return FAVOURITE_COLOR_INPUT;
    }

    @Override
    public FavoriteColorStreamApplication createStreamingApplication() {
        return new FavoriteColorStreamApplication();
    }

    @Test
    public void favoriteColorsWithUpdateOnBlue(){
        sendMessage("stephane,blue");
        OutputVerifier.compareKeyValue(readOutput(), "blue", 1L);
        sendMessage("john,green");
        OutputVerifier.compareKeyValue(readOutput(), "green", 1L);
        sendMessage("stephane,red");
        OutputVerifier.compareKeyValue(readOutput(), "blue", 0L);
        OutputVerifier.compareKeyValue(readOutput(), "red", 1L);
        sendMessage("alice,red");
        OutputVerifier.compareKeyValue(readOutput(), "red", 2L);
    }

    private ProducerRecord<String, Long> readOutput() {
        return this.testDriver.readOutput(FAVOURITE_COLOR_OUTPUT, new StringDeserializer(), new LongDeserializer());
    }
}