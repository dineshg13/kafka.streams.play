package com.home.dedup;

import com.home.config.IKafkaConstants;
import com.home.models.Tweet;
import com.home.stream.join.StreamTopology;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

public class StreamTesting {

    @Test
    public void testStream() {


        Topology topology = StreamTopology.builder().build();
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test3");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        TopologyTestDriver testDriver = new TopologyTestDriver(topology, config);

        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(
                IKafkaConstants.IN_TOPIC_NAME,
                new StringSerializer(),
                new StringSerializer());

        TestInputTopic<String, String> errorTopic = testDriver.createInputTopic(
                IKafkaConstants.ERROR_TOPIC_NAME,
                new StringSerializer(),
                new StringSerializer());

        TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(
                IKafkaConstants.OUT_TOPIC_NAME,
                new StringDeserializer(),
                new StringDeserializer());

        Tweet t1 = new Tweet(10L, "10", Instant.now());
        Tweet t2 = new Tweet(10L, "10", Instant.now());
        Tweet t3 = new Tweet(11L, "10", Instant.now());

        inputTopic.pipeInput(UUID.randomUUID().toString(), t1.serialize());
        errorTopic.pipeInput(UUID.randomUUID().toString(), t2.serialize());
        errorTopic.pipeInput(UUID.randomUUID().toString(), t3.serialize());


        System.out.println("#### OUTPUT :" + outputTopic.readKeyValue());

        testDriver.close();

    }
}
