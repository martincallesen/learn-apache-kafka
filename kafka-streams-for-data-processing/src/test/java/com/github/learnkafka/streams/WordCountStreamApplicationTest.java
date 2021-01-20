package com.github.learnkafka.streams;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WordCountStreamApplicationTest {

    @Test
    void multipleWords() {
        String msg1 = "testing Kafka Streams";
        String msg2 = "testing Kafka again";

        Assertions.assertEquals("kafka count 2", null, "Failed to count with multiple words");
        Assertions.assertEquals("testing count 2", null, "Failed to count with multiple words");
        Assertions.assertEquals("Streams count 1", null, "Failed to count with multiple words");
        Assertions.assertEquals("again count 1", null, "Failed to count with multiple words");
    }

    @Test
    void lowercaseWords(){
        String msg2 = "KAFKA Kafka kafka";

        Assertions.assertEquals("kafka count 3", null, "Failed to count with lowercase words");
    }
}