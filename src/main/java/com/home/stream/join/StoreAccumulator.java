package com.home.stream.join;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;


public class StoreAccumulator implements Transformer<String, String, KeyValue<String, String>> {

    private String storeName;
    private static Logger logger = LoggerFactory.getLogger(StoreAccumulator.class);

    private ProcessorContext context;
    private final Duration scanFrequency;
    private final Duration ttl;


    /**
     * Key: event ID
     * Value: timestamp (event-time) of the corresponding event when the event ID was seen for the
     * first time
     */
    private KeyValueStore<String, Long> stateStore;

    public StoreAccumulator(String storeName, Duration scanFrequency, Duration ttl) {
        this.storeName = storeName;
        this.scanFrequency = scanFrequency;
        this.ttl = ttl;

    }


    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        stateStore = context.getStateStore(storeName);
        context.schedule(this.scanFrequency, PunctuationType.STREAM_TIME, new Punctuator() {
            @Override
            public void punctuate(long timestamp) {
                try (final KeyValueIterator<String, Long> all = stateStore.all()) {
                    while (all.hasNext()) {
                        final KeyValue<String, Long> record = all.next();
                        if (record.value != null && record.value < timestamp) {
                            logger.info("in punctuate Forwarding Null");
                            // if a record's last update was older than our cutoff, emit a tombstone.
                            context.forward(record.key, null);
                        }
                    }
                }
            }
        });
    }

    @Override
    public KeyValue<String, String> transform(String key, String value) {
        // it from our state store. Otherwise, store the record timestamp.
        if (value == null) {
            logger.info("CLEANING key=" + key);
            stateStore.delete(key);
        } else {
            logger.info("UPDATING key=" + key);
            stateStore.put(key, context.timestamp() + ttl.get(ChronoUnit.SECONDS));
        }
        return null;
    }

    @Override
    public void close() {

    }
}
