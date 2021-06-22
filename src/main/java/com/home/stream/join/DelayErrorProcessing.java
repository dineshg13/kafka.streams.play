package com.home.stream.join;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelayErrorProcessing implements Transformer<String, String, KeyValue<String, String>> {

    private String storeName;
    private static Logger logger = LoggerFactory.getLogger(DelayErrorProcessing.class);

    private ProcessorContext context;
    private KeyValueStore<String, Long> stateStore;

    public DelayErrorProcessing(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        stateStore = context.getStateStore(storeName);
    }

    @Override
    public KeyValue<String, String> transform(String key, String value) {
        Long val = null;
        for (int i = 0; i < 3; i++) {
            val = stateStore.get(key);

            if (val != null) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        if (val == null) {
            return KeyValue.pair(key, value);
        }
        return null;
    }

    @Override
    public void close() {

    }
}
