package com.home.config;

public class IKafkaConstants {
    public static String KAFKA_BROKERS = "localhost:9092";
    public static String PRODUCER_ID = "producer1";

    public static String IN_TOPIC_NAME = "dedupe-in";
    public static String OUT_TOPIC_NAME = "dedupe-out";

    public static String GROUP_ID_CONFIG = "consumerGroup1";

    public static String OFFSET_RESET_LATEST = "latest";

    public static String OFFSET_RESET_EARLIER = "earliest";

    public static Integer MAX_POLL_RECORDS = 1;

    public static String STORE_NAME = "tweet_id_store";
    public static Integer NUM_OF_PARTITIONS = 32;
}
