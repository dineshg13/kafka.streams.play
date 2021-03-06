package com.home.dedup;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeduplicationTransformer<K, V, E> implements Transformer<K, V, KeyValue<K, V>> {
    private String storeName;
    private static Logger logger = LoggerFactory.getLogger(DeduplicationTransformer.class);

    private ProcessorContext context;

    /**
     * Key: event ID
     * Value: timestamp (event-time) of the corresponding event when the event ID was seen for the
     * first time
     */
    private WindowStore<E, Long> eventIdStore;

    private final long leftDurationMs;
    private final long rightDurationMs;

    private final KeyValueMapper<K, V, E> idExtractor;

    /**
     * @param maintainDurationPerEventInMs how long to "remember" a known event (or rather, an event
     *                                     ID), during the time of which any incoming duplicates of
     *                                     the event will be dropped, thereby de-duplicating the
     *                                     input.
     * @param idExtractor                  extracts a unique identifier from a record by which we de-duplicate input
     *                                     records; if it returns null, the record will not be considered for
     *                                     de-duping but forwarded as-is.
     */
    public DeduplicationTransformer(String storeName, final long maintainDurationPerEventInMs, final KeyValueMapper<K, V, E> idExtractor) {
        this.storeName = storeName;
        if (maintainDurationPerEventInMs < 1) {
            throw new IllegalArgumentException("maintain duration per event must be >= 1");
        }
        leftDurationMs = maintainDurationPerEventInMs / 2;
        rightDurationMs = maintainDurationPerEventInMs - leftDurationMs;
        this.idExtractor = idExtractor;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        this.context = context;
        eventIdStore = (WindowStore<E, Long>) context.getStateStore(storeName);
    }

    public KeyValue<K, V> transform(final K key, final V value) {
        final E eventId = idExtractor.apply(key, value);
        boolean isDuplicate = isDuplicate(eventId);
        logger.info("processing eventID:" + eventId + " time:" + context.timestamp() + " , isDuplicate:" + isDuplicate + ", leftDurationMs : " + leftDurationMs + ", rightDurationMs:" + rightDurationMs + ", partition:" + context.partition());
        if (eventId == null) {
            return KeyValue.pair(key, value);
        } else {
            final KeyValue<K, V> output;
            if (isDuplicate) {
                output = null;
                updateTimestampOfExistingEventToPreventExpiry(eventId, context.timestamp());
            } else {
                output = KeyValue.pair(key, value);
                rememberNewEvent(eventId, context.timestamp());
            }
            return output;
        }
    }

    private boolean isDuplicate(final E eventId) {
        final long eventTime = context.timestamp();

        final WindowStoreIterator<Long> timeIterator = eventIdStore.fetch(
                eventId,
                eventTime - leftDurationMs,
                eventTime + rightDurationMs);
        final boolean isDuplicate = timeIterator.hasNext();
        timeIterator.close();
        return isDuplicate;
    }

    private void updateTimestampOfExistingEventToPreventExpiry(final E eventId, final long newTimestamp) {
        eventIdStore.put(eventId, newTimestamp, newTimestamp);
    }

    private void rememberNewEvent(final E eventId, final long timestamp) {
        eventIdStore.put(eventId, timestamp, timestamp);
    }

    @Override
    public void close() {
        // Note: The store should NOT be closed manually here via `eventIdStore.close()`!
        // The Kafka Streams API will automatically close stores when necessary.
    }

}