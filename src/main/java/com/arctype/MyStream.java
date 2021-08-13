package com.arctype;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class MyStream {
    private final static String BOOTSTRAP_SERVERS = "localhost:29092";
    private final static String APPLICATION_ID = "arctype-stream";

    private static Properties makeProps() {
        final Properties props = new Properties();

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        props.put(StreamsConfig.POLL_MS_CONFIG, 100);
        props.put(StreamsConfig.RETRIES_CONFIG, 100);
        return props;
    }

    private static Topology createTopology(Properties props) {
        StreamsBuilder builder = new StreamsBuilder();
        // Add your streams here.
        TradeStream.build(builder);
        final Topology topology = builder.build();
        System.out.println(topology.describe());
        return topology;
    }


    public static void main(String[] args) throws Exception {
        final Properties props = makeProps();
        final Topology topology = createTopology(props);
        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run(){
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
