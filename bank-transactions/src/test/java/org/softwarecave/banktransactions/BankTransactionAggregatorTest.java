package org.softwarecave.banktransactions;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class BankTransactionAggregatorTest {

    private TopologyTestDriver testDriver;
    private JsonMapper jsonMapper;

    @BeforeEach
    public void setup() {
        BankTransactionAggregator bankTransactionAggregator = new BankTransactionAggregator();
        Topology topology = bankTransactionAggregator.createTopology();
        testDriver = new TopologyTestDriver(topology);
        jsonMapper = new JsonMapper();
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void testBankTransactionsNoClients() {
        var outputTopic = getOutputTopic();

        Map<String, String> outputMap = outputTopic.readKeyValuesToMap();
        assertThat(outputMap.size()).isEqualTo(0);
    }

    @Test
    public void testBankTransactionsOneClient() {
        var inputTopic = getInputTopic();
        var outputTopic = getOutputTopic();

        inputTopic.pipeInput("John", createInputTopicJson("John", BigDecimal.valueOf(50.0), LocalDateTime.of(2024, 6, 1, 10, 0)));

        Map<String, String> outputMap = outputTopic.readKeyValuesToMap();
        assertThat(outputMap.size()).isEqualTo(1);
        assertThat(outputMap.get("John"))
                .isNotNull()
                .contains(createPairJson("clientName", "John"))
                .contains(createPairJson("totalAmount", BigDecimal.valueOf(50.0)))
                .contains(createPairJson("transactionCount", BigDecimal.valueOf(1)))
                .contains(createPairJson("latestTransactionDateTime", "2024-06-01T10:00:00"));
    }


    @Test
    public void testBankTransactionsTwoClients() {
        var inputTopic = getInputTopic();
        var outputTopic = getOutputTopic();

        inputTopic.pipeInput("John", createInputTopicJson("John", BigDecimal.valueOf(50.0), LocalDateTime.of(2024, 6, 1, 10, 0)));
        inputTopic.pipeInput("John", createInputTopicJson("John", BigDecimal.valueOf(25.0), LocalDateTime.of(2024, 6, 1, 11, 0)));
        inputTopic.pipeInput("Jane", createInputTopicJson("Jane", BigDecimal.valueOf(30.0), LocalDateTime.of(2024, 6, 1, 12, 0)));

        Map<String, String> outputMap = outputTopic.readKeyValuesToMap();
        assertThat(outputMap).hasSize(2);
        assertThat(outputMap.get("John"))
                .isNotNull()
                .contains(createPairJson("clientName", "John"))
                .contains(createPairJson("totalAmount", BigDecimal.valueOf(75.0)))
                .contains(createPairJson("transactionCount", BigDecimal.valueOf(2)))
                .contains(createPairJson("latestTransactionDateTime", "2024-06-01T11:00:00"));
        assertThat(outputMap.get("Jane"))
                .isNotNull()
                .contains(createPairJson("clientName", "Jane"))
                .contains(createPairJson("totalAmount", BigDecimal.valueOf(30.0)))
                .contains(createPairJson("transactionCount", BigDecimal.valueOf(1)))
                .contains(createPairJson("latestTransactionDateTime", "2024-06-01T12:00:00"));

    }

    private TestInputTopic<String, String> getInputTopic() {
        return testDriver.createInputTopic(BankTransactionAggregator.BANK_TRANSACTIONS_TOPIC_NAME,
                new StringSerializer(), new StringSerializer());
    }

    private TestOutputTopic<String, String> getOutputTopic() {
        return testDriver.createOutputTopic(BankTransactionAggregator.BANK_TRANSACTIONS_AGGREGATED_TOPIC_NAME,
                new StringDeserializer(), new StringDeserializer());
    }

    private String createInputTopicJson(String name, BigDecimal amount, LocalDateTime transactionDateTime) {
        BankTransaction bankTransaction = new BankTransaction(name, amount, transactionDateTime);
        return jsonMapper.writeValueAsString(bankTransaction);
    }

    private String createPairJson(String key, String value) {
        return "\"%s\":\"%s\"".formatted(key, value);
    }

    private String createPairJson(String key, BigDecimal value) {
        return "\"%s\":%s".formatted(key, value);
    }
}
