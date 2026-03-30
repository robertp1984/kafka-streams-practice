package org.softwarecave.banktransactions;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class BankTransactionAggregator {

    public static final String BANK_TRANSACTIONS_TOPIC_NAME = "bank.transactions";
    public static final String BANK_TRANSACTIONS_AGGREGATED_TOPIC_NAME = "bank.transactions.aggregated";

    public static void main() throws InterruptedException {
        new BankTransactionAggregator().run();
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
        StreamsBuilder builder = new StreamsBuilder();
        String initialAggregatedValue = createInitialAggregatedValue();

        KTable<String, String> table = builder.stream(BANK_TRANSACTIONS_TOPIC_NAME, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(() -> initialAggregatedValue,
                        (k, v, aggregatedValue) -> aggregateNewValue(v, aggregatedValue),
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("aggr").withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
                );

        table.toStream().to(BANK_TRANSACTIONS_AGGREGATED_TOPIC_NAME, Produced.with(Serdes.String(), Serdes.String()));
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

    private String aggregateNewValue(String bankTransactionJson, String aggregatedJson) {
        JsonMapper mapper = new JsonMapper();
        BankTransactionAggregated aggregated = mapper.readValue(aggregatedJson, BankTransactionAggregated.class);
        BankTransaction bankTransaction = mapper.readValue(bankTransactionJson, BankTransaction.class);

        aggregated.setTransactionCount(aggregated.getTransactionCount() + 1);
        aggregated.setClientName(bankTransaction.getClientName());
        aggregated.setTotalAmount(aggregated.getTotalAmount().add(bankTransaction.getAmount()));
        if (bankTransaction.getTransactionDateTime().isAfter(aggregated.getLatestTransactionDateTime())) {
            aggregated.setLatestTransactionDateTime(bankTransaction.getTransactionDateTime());
        }

        return mapper.writeValueAsString(aggregated);
    }

    public String createInitialAggregatedValue() {
        JsonMapper mapper = new JsonMapper();

        BankTransactionAggregated bankTransactionAggregated = BankTransactionAggregated.builder()
                .clientName("unknown")
                .totalAmount(BigDecimal.ZERO)
                .transactionCount(0)
                .latestTransactionDateTime(LocalDateTime.MIN)
                .build();

        return mapper.writeValueAsString(bankTransactionAggregated);
    }
}
