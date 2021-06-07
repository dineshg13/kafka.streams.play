package com.home.dedup;

import com.home.config.IKafkaConstants;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

public class TweetConsumer {
    Consumer<Long, String> kafkaConsumer;
    private static Logger logger = LoggerFactory.getLogger(TweetConsumer.class);

    public Consumer<Long, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, IKafkaConstants.GROUP_ID_CONFIG);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, IKafkaConstants.MAX_POLL_RECORDS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, IKafkaConstants.OFFSET_RESET_EARLIER);

        Consumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(IKafkaConstants.OUT_TOPIC_NAME));
        return consumer;
    }

    public void runConsumer() {

        int noMessageFound = 0;

        while (true) {
            ConsumerRecords<Long, String> consumerRecords = kafkaConsumer.poll(Duration.of(1, ChronoUnit.SECONDS));
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.

            //print each record.
            consumerRecords.forEach(record -> {
                logger.info(
                        "Record Key " + record.key() +
                                ", Record value " + record.value() +
                                ", Record partition " + record.partition() +
                                ", Record offset " + record.offset());
            });

            // commits the offset of record to broker.
            kafkaConsumer.commitAsync();
        }
    }

    public TweetConsumer() {
        kafkaConsumer = createConsumer();
    }

    public static void main(String[] args) {
        TweetConsumer consumer = new TweetConsumer();
        consumer.runConsumer();
    }
}
