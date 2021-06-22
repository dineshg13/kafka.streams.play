package com.home.stream.join;

import com.home.config.IKafkaConstants;
import com.home.models.Tweet;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StreamTopology {

    public static StreamsBuilder builder() {
        Properties props = new Properties();
        String APPLICATION_ID = "streams-dedup-41";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put("auto.offset.reset", "latest");

        final StreamsBuilder builder = new StreamsBuilder();
        Map<String, String> changeLogConfigs = new HashMap<>();
        changeLogConfigs.put("retention.ms", "172800000");
        changeLogConfigs.put("retention.bytes", "10000000000");
        changeLogConfigs.put("cleanup.policy", "compact,delete");
        changeLogConfigs.put("topic", "changelog");


        final StoreBuilder<KeyValueStore<String, Long>> stateStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(IKafkaConstants.STORE_NAME),
                Serdes.String(),
                Serdes.Long());
        stateStoreBuilder.withLoggingEnabled(changeLogConfigs);


        builder.addStateStore(stateStoreBuilder);
        final Duration scanFreq = Duration.of(5, ChronoUnit.MINUTES);
        final Duration ttl = Duration.of(24, ChronoUnit.HOURS);

        builder.stream(IKafkaConstants.IN_TOPIC_NAME, Consumed.with(Serdes.String(), Serdes.String()))
                .selectKey((key, value) -> {
                    Tweet t = Tweet.Deserialize(value);
                    return t.getId().toString();
                })
                .repartition(Repartitioned.with(Serdes.String(), Serdes.String()).withName(""))
                .transform(() -> new StoreAccumulator(IKafkaConstants.STORE_NAME, scanFreq, ttl), IKafkaConstants.STORE_NAME);

        builder.stream(IKafkaConstants.ERROR_TOPIC_NAME, Consumed.with(Serdes.String(), Serdes.String()))
                .selectKey((key, value) -> {
                    Tweet t = Tweet.Deserialize(value);
                    return t.getId().toString();
                })
                .repartition(Repartitioned.with(Serdes.String(), Serdes.String()).withName("error"))
                .transform(() -> new DelayErrorProcessing(IKafkaConstants.STORE_NAME), IKafkaConstants.STORE_NAME)
                .to(IKafkaConstants.OUT_TOPIC_NAME, Produced.with(Serdes.String(), Serdes.String()));
        ;

        return builder;

    }

}
