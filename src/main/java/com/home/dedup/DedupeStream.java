package com.home.dedup;

import com.home.config.IKafkaConstants;
import com.home.models.Tweet;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class DedupeStream {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        String APPLICATION_ID = "streams-dedup-41";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put("auto.offset.reset", "latest");

        final StreamsBuilder builder = new StreamsBuilder();
        final Duration windowSize = Duration.of(5, ChronoUnit.MINUTES);
        final Duration retention = Duration.of(10, ChronoUnit.MINUTES);
        Map<String, String> changeLogConfigs = new HashMap<>();
        changeLogConfigs.put("retention.ms", "172800000");
        changeLogConfigs.put("retention.bytes", "10000000000");
        changeLogConfigs.put("cleanup.policy", "compact,delete");
        changeLogConfigs.put("topic", "changelog");

        final StoreBuilder<WindowStore<Long, Long>> dedupStoreBuilder = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(IKafkaConstants.STORE_NAME,
                        retention,
                        windowSize,
                        false
                ),
                Serdes.Long(),
                Serdes.Long());
        dedupStoreBuilder.withLoggingEnabled(changeLogConfigs);

        builder.addStateStore(dedupStoreBuilder);


        builder.<String, String>stream(IKafkaConstants.IN_TOPIC_NAME)
                .selectKey(new KeyValueMapper<String, String, String>() {

                    @Override
                    public String apply(String key, String value) {
                        Tweet t = Tweet.Deserialize(value);
                        return t.getId().toString();
                    }
                })
                .repartition(Repartitioned.as(""))
                .transform(() -> new DeduplicationTransformer<>(IKafkaConstants.STORE_NAME, windowSize.get(ChronoUnit.SECONDS) * 1000, (key, value) -> {
                    Tweet t = Tweet.Deserialize(value);
                    return t.getId();
                }
                ), IKafkaConstants.STORE_NAME)
                .to(IKafkaConstants.OUT_TOPIC_NAME);

        ;


        final Topology topology = builder.build();
        System.out.println(topology.describe());
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

}
