package org.softwarecave.movieticketcounter;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.softwarecave.movieticketcounter.avro.TicketSellEvent;
import org.softwarecave.movieticketcounter.avro.TicketSoldSummary;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

public class TicketCounterTest {

    private MockSchemaRegistryClient srClient;
    private SpecificAvroSerde<TicketSellEvent> ticketSellEventSerde;
    private SpecificAvroSerde<TicketSoldSummary> ticketSoldSummarySerde;

    private TopologyTestDriver testDriver;

    @BeforeEach
    public void setUp() {
        TicketCounter ticketCounter = new TicketCounter();
        var config = ticketCounter.createConfig();

        srClient = new MockSchemaRegistryClient();
        ticketSellEventSerde = createAvroSerde(TicketSellEvent.class, config, srClient);
        ticketSoldSummarySerde = createAvroSerde(TicketSoldSummary.class, config, srClient);
        var topology = ticketCounter.createTopology(config, ticketSellEventSerde, ticketSoldSummarySerde);

        testDriver = new TopologyTestDriver(topology, config);
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void testOneEvent() {
        var inputTopic = createInputTopic();
        var outputTopic = createOutputTopic();

        inputTopic.pipeInput(null, new TicketSellEvent("Good Movie", 2, List.of("3A", "3B")));

        var outputTopicMap = outputTopic.readKeyValuesToMap();
        assertThat(outputTopicMap.size()).isEqualTo(1);
        assertThat(outputTopicMap).containsKey("Good Movie");
        assertThat(outputTopicMap.get("Good Movie"))
                .hasFieldOrPropertyWithValue("movieTitle", "Good Movie")
                .hasFieldOrPropertyWithValue("ticketsSoldTotal", 2L)
                .hasFieldOrPropertyWithValue("seatsSold", List.of("3A", "3B"));
    }

    @Test
    public void testSixEventsAndThreeMovies() {
        var inputTopic = createInputTopic();
        var outputTopic = createOutputTopic();

        inputTopic.pipeInput(null, new TicketSellEvent("Good Movie", 2, List.of("3A", "3B")));
        inputTopic.pipeInput(null, new TicketSellEvent("Medium Movie", 2, List.of("4F", "4G")));
        inputTopic.pipeInput(null, new TicketSellEvent("Medium Movie", 1, List.of("5A")));
        inputTopic.pipeInput(null, new TicketSellEvent("Good Movie", 4, List.of("6A", "6B", "6C", "6D")));
        inputTopic.pipeInput(null, new TicketSellEvent("Awesome Movie", 1, List.of("7A")));
        inputTopic.pipeInput(null, new TicketSellEvent("Good Movie", 5, List.of("8A", "8B", "8D", "8E", "8F")));

        var outputTopicMap = outputTopic.readKeyValuesToMap();
        assertThat(outputTopicMap.size()).isEqualTo(3);

        assertThat(outputTopicMap.get("Good Movie"))
                .hasFieldOrPropertyWithValue("movieTitle", "Good Movie")
                .hasFieldOrPropertyWithValue("ticketsSoldTotal", 11L)
                .hasFieldOrPropertyWithValue("seatsSold", List.of("3A", "3B", "6A", "6B", "6C", "6D", "8A", "8B", "8D", "8E", "8F"));

        assertThat(outputTopicMap.get("Medium Movie"))
                .hasFieldOrPropertyWithValue("movieTitle", "Medium Movie")
                .hasFieldOrPropertyWithValue("ticketsSoldTotal", 3L)
                .hasFieldOrPropertyWithValue("seatsSold", List.of("4F", "4G", "5A"));

        assertThat(outputTopicMap.get("Awesome Movie"))
                .hasFieldOrPropertyWithValue("movieTitle", "Awesome Movie")
                .hasFieldOrPropertyWithValue("ticketsSoldTotal", 1L)
                .hasFieldOrPropertyWithValue("seatsSold", List.of("7A"));
    }

    private TestInputTopic<String, TicketSellEvent> createInputTopic() {
        return testDriver.createInputTopic(TicketCounter.INPUT_TOPIC, new StringSerializer(),
                ticketSellEventSerde.serializer());
    }

    private TestOutputTopic<String, TicketSoldSummary> createOutputTopic() {
        return testDriver.createOutputTopic(TicketCounter.OUTPUT_TOPIC, new StringDeserializer(),
                ticketSoldSummarySerde.deserializer());
    }

    <T extends SpecificRecord> SpecificAvroSerde<T> createAvroSerde(Class<T> klass, Properties config,
                                                                    SchemaRegistryClient srClient) {
        final SpecificAvroSerde<T> serde = new SpecificAvroSerde<>(srClient);
        serde.configure(Collections.singletonMap(
                SCHEMA_REGISTRY_URL_CONFIG, config.getProperty(SCHEMA_REGISTRY_URL_CONFIG)
        ), false);
        return serde;
    }
}
