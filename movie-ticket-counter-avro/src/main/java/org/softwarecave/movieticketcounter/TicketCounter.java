package org.softwarecave.movieticketcounter;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.softwarecave.movieticketcounter.avro.TicketSellEvent;
import org.softwarecave.movieticketcounter.avro.TicketSoldSummary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

@Slf4j
public class TicketCounter {

    public static final String INPUT_TOPIC = "ticket-sell-event";
    public static final String OUTPUT_TOPIC = "ticket-sold-summary";

    public static void main() throws InterruptedException {
        TicketCounter ticketCounter = new TicketCounter();
        ticketCounter.run();
    }

    public void run() throws InterruptedException {
        var config = createConfig();

        SpecificAvroSerde<TicketSellEvent> ticketSellEventSerde = createAvroSerde(TicketSellEvent.class, config);
        SpecificAvroSerde<TicketSoldSummary> ticketSoldSummarySerde = createAvroSerde(TicketSoldSummary.class, config);
        var topology = createTopology(config, ticketSellEventSerde, ticketSoldSummarySerde);

        // intentionally not using try-with-resources here to keep the application running until shutdown
        KafkaStreams streams = new KafkaStreams(topology, config);
        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Stop requested. Exiting.");
            streams.close();
            latch.countDown();
        }));

        streams.start();
        latch.await();
    }

    Properties createConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "movie-ticket-counter-avro");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        return props;
    }

    Topology createTopology(Properties config, SpecificAvroSerde<TicketSellEvent> ticketSellEventSerde, SpecificAvroSerde<TicketSoldSummary> ticketSoldSummarySerde) {
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, TicketSellEvent> inputStream = builder.stream(INPUT_TOPIC,
                Consumed.with(stringSerde, ticketSellEventSerde));

        KTable<String, TicketSoldSummary> aggregatedTable = inputStream
                .peek((k, v) -> log.info("Input event: {}:{}", k, v))
                .filter((k, v) -> v.getTicketCount() > 0
                        && v.getTicketCount() == v.getSeats().size())
                .selectKey((k, v) -> v.getMovieTitle())
                .groupByKey()
                .aggregate(this::createInitialTicketSoldSummary,
                        (k, v, summary) -> addToTicketSoldSummary(summary, k, v),
                        Materialized.<String, TicketSoldSummary, KeyValueStore<Bytes, byte[]>>as("aggr")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(ticketSoldSummarySerde));

        aggregatedTable.toStream().to(OUTPUT_TOPIC, Produced.with(stringSerde, ticketSoldSummarySerde));

        return builder.build();
    }

    private TicketSoldSummary addToTicketSoldSummary(TicketSoldSummary summary, String k, TicketSellEvent v) {
        var seatsSold = new ArrayList<>(summary.getSeatsSold());
        seatsSold.addAll(v.getSeats());

        var result = TicketSoldSummary.newBuilder()
                .setMovieTitle(k)
                .setTicketsSoldTotal(summary.getTicketsSoldTotal() + v.getTicketCount())
                .setSeatsSold(seatsSold)
                .build();
        return result;
    }

    private TicketSoldSummary createInitialTicketSoldSummary() {
        return TicketSoldSummary.newBuilder()
                .setMovieTitle("")
                .setTicketsSoldTotal(0L)
                .setSeatsSold(List.of())
                .build();
    }

    <T extends SpecificRecord> SpecificAvroSerde<T> createAvroSerde(Class<T> klass, Properties config) {
        final SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(Collections.singletonMap(
                SCHEMA_REGISTRY_URL_CONFIG, config.getProperty(SCHEMA_REGISTRY_URL_CONFIG)
        ), false);
        return serde;
    }
}

