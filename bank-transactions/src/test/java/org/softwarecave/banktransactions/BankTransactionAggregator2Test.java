package org.softwarecave.banktransactions;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.softwarecave.org.softwarecave.kafkastreamscommon.JsonSerde;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class BankTransactionAggregator2Test {

    private TopologyTestDriver testDriver;
    private Serde<BankTransaction> bankTransactionJsonSerde;
    private Serde<BankTransactionAggregated> bankTransactionAggregatedJsonSerde;

    @BeforeEach
    public void setup() {
        BankTransactionAggregator2 bankTransactionAggregator2 = new BankTransactionAggregator2();
        Topology topology = bankTransactionAggregator2.createTopology();
        testDriver = new TopologyTestDriver(topology);

        bankTransactionJsonSerde = new JsonSerde<>(BankTransaction.class);
        bankTransactionAggregatedJsonSerde = new JsonSerde<>(BankTransactionAggregated.class);
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void testBankTransactionsNoClients() {
        var outputTopic = getOutputTopic();

        Map<String, BankTransactionAggregated> outputMap = outputTopic.readKeyValuesToMap();
        assertThat(outputMap.size()).isEqualTo(0);
    }

    @Test
    public void testBankTransactionsOneClient() {
        var inputTopic = getInputTopic();
        var outputTopic = getOutputTopic();

        inputTopic.pipeInput("John", new BankTransaction("John", BigDecimal.valueOf(50.0), LocalDateTime.of(2024, 6, 1, 10, 0)));

        Map<String, BankTransactionAggregated> outputMap = outputTopic.readKeyValuesToMap();
        assertThat(outputMap.size()).isEqualTo(1);

        assertThat(outputMap.get("John"))
                .isNotNull()
                .hasFieldOrPropertyWithValue("clientName", "John")
                .hasFieldOrPropertyWithValue("totalAmount", BigDecimal.valueOf(50.0))
                .hasFieldOrPropertyWithValue("transactionCount", 1L)
                .hasFieldOrPropertyWithValue("latestTransactionDateTime", LocalDateTime.parse("2024-06-01T10:00:00"));
    }


    @Test
    public void testBankTransactionsTwoClients() {
        var inputTopic = getInputTopic();
        var outputTopic = getOutputTopic();

        inputTopic.pipeInput("John", new BankTransaction("John", BigDecimal.valueOf(50.0), LocalDateTime.of(2024, 6, 1, 10, 0)));
        inputTopic.pipeInput("John", new BankTransaction("John", BigDecimal.valueOf(25.0), LocalDateTime.of(2024, 6, 1, 11, 0)));
        inputTopic.pipeInput("Jane", new BankTransaction("Jane", BigDecimal.valueOf(30.0), LocalDateTime.of(2024, 6, 1, 12, 0)));

        Map<String, BankTransactionAggregated> outputMap = outputTopic.readKeyValuesToMap();
        assertThat(outputMap).hasSize(2);
        assertThat(outputMap.get("John"))
                .isNotNull()
                .hasFieldOrPropertyWithValue("clientName", "John")
                .hasFieldOrPropertyWithValue("totalAmount", BigDecimal.valueOf(75.0))
                .hasFieldOrPropertyWithValue("transactionCount", 2L)
                .hasFieldOrPropertyWithValue("latestTransactionDateTime", LocalDateTime.parse("2024-06-01T11:00:00"));
        assertThat(outputMap.get("Jane"))
                .isNotNull()
                .hasFieldOrPropertyWithValue("clientName", "Jane")
                .hasFieldOrPropertyWithValue("totalAmount", BigDecimal.valueOf(30.0))
                .hasFieldOrPropertyWithValue("transactionCount", 1L)
                .hasFieldOrPropertyWithValue("latestTransactionDateTime", LocalDateTime.parse("2024-06-01T12:00:00"));

    }

    private TestInputTopic<String, BankTransaction> getInputTopic() {
        return testDriver.createInputTopic(BankTransactionAggregator.BANK_TRANSACTIONS_TOPIC_NAME,
                new StringSerializer(), bankTransactionJsonSerde.serializer());
    }

    private TestOutputTopic<String, BankTransactionAggregated> getOutputTopic() {
        return testDriver.createOutputTopic(BankTransactionAggregator.BANK_TRANSACTIONS_AGGREGATED_TOPIC_NAME,
                new StringDeserializer(), bankTransactionAggregatedJsonSerde.deserializer());
    }

}
