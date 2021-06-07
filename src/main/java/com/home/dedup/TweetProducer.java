package com.home.dedup;

import com.home.config.IKafkaConstants;
import com.home.models.Tweet;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;

public class TweetProducer {
    private String topic;

    public Properties getProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, IKafkaConstants.PRODUCER_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return props;

    }

    KafkaProducer<String, String> kafkaProducer;


    public TweetProducer(String topic) {
        this.kafkaProducer = new KafkaProducer<>(getProperties());
        this.topic = topic;

    }

    private static final Logger logger = LoggerFactory.getLogger(TweetProducer.class);

    public void sendTweets(int n) {

        for (int i = 0; i < n; i++) {
            Tweet t = new Tweet(Long.valueOf(i % 5), String.valueOf(i), Instant.now());
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, i % IKafkaConstants.NUM_OF_PARTITIONS, t.getId().toString(), t.Serialize());
            try {
                RecordMetadata metadata = this.kafkaProducer.send(record).get();
                logger.info("message No: " + i + ", topic:" + metadata.topic() + " partition:" + metadata.partition() + " ,offset:" + metadata.offset());
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

    }

    public static void main(String[] args) {
        TweetProducer tweetProducer = new TweetProducer(IKafkaConstants.IN_TOPIC_NAME);
        tweetProducer.sendTweets(10);
    }

}
