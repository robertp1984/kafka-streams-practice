package org.softwarecave.banktransactions;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.softwarecave.org.softwarecave.kafkastreamscommon.JsonSerde;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Streams application that aggregates bank transactions by client name.
 * This class utilizes the Json Serde instead of parsing the JSON strings manually.
 */
@Slf4j
public class BankTransactionAggregator2 {

    public static final String BANK_TRANSACTIONS_TOPIC_NAME = "bank.transactions";
    public static final String BANK_TRANSACTIONS_AGGREGATED_TOPIC_NAME = "bank.transactions.aggregated";

    public static void main() throws InterruptedException {
        new BankTransactionAggregator2().run();
    }

    public void run() throws InterruptedException {
        Properties props = createConfig();

        Topology topology = createTopology();

        // Start Kafka Streams application with shutdown hook
        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Stop requested. Exiting.");
            streams.close();
            latch.countDown();
        }));

        streams.start();
        latch.await();
    }

    Topology createTopology() {
        final Serde<BankTransaction> bankTransactionJsonSerde = new JsonSerde<>(BankTransaction.class);
        final Serde<BankTransactionAggregated> bankTransactionAggregatedJsonSerde = new JsonSerde<>(BankTransactionAggregated.class);

        final BankTransactionAggregated initialAggregatedValue = createInitialAggregatedValue();

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, BankTransactionAggregated> table = builder
                .stream(BANK_TRANSACTIONS_TOPIC_NAME, Consumed.with(Serdes.String(), bankTransactionJsonSerde))
                .groupByKey(Grouped.with(Serdes.String(), bankTransactionJsonSerde))
                .aggregate(() -> initialAggregatedValue,
                        (k, v, aggregatedValue) -> aggregateNewValue(v, aggregatedValue),
                        Materialized.<String, BankTransactionAggregated, KeyValueStore<Bytes, byte[]>>as("aggr2")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(bankTransactionAggregatedJsonSerde)
                );

        table.toStream()
                .to(BANK_TRANSACTIONS_AGGREGATED_TOPIC_NAME,
                        Produced.with(Serdes.String(), bankTransactionAggregatedJsonSerde));
        return builder.build();
    }

    private static Properties createConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-transaction-aggregator");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private BankTransactionAggregated aggregateNewValue(BankTransaction bankTransaction, BankTransactionAggregated aggregatedPrev) {
        var aggregated = new BankTransactionAggregated(aggregatedPrev.getClientName(), aggregatedPrev.getTotalAmount(),
                aggregatedPrev.getTransactionCount(), aggregatedPrev.getLatestTransactionDateTime());

        aggregated.setTransactionCount(aggregated.getTransactionCount() + 1);
        aggregated.setClientName(bankTransaction.getClientName());
        aggregated.setTotalAmount(aggregated.getTotalAmount().add(bankTransaction.getAmount()));
        if (bankTransaction.getTransactionDateTime().isAfter(aggregated.getLatestTransactionDateTime())) {
            aggregated.setLatestTransactionDateTime(bankTransaction.getTransactionDateTime());
        }
        return aggregated;
    }

    public BankTransactionAggregated createInitialAggregatedValue() {
        return BankTransactionAggregated.builder()
                .clientName("unknown")
                .totalAmount(BigDecimal.ZERO)
                .transactionCount(0)
                .latestTransactionDateTime(LocalDateTime.MIN)
                .build();
    }
}
