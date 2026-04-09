package org.softwarecave.movieticketcounter;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.softwarecave.movieticketcounter.avro.TicketSellEvent;

import java.util.List;
import java.util.Properties;

public class TicketSellEventProducer {

    public static final String TICKET_SELL_EVENT_TOPIC_NAME = "ticket.sellEvent";

    public static void main() throws InterruptedException {
        new TicketSellEventProducer().run();
    }

    public void run() throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");
        // The other settings are not needed as defaults are fine in recent Kafka versions


        try (KafkaProducer<String, TicketSellEvent> producer = new KafkaProducer<>(props)) {
            sendTicketSellEvent("Forrest Gump", List.of("A1", "A2"), producer);
            sendTicketSellEvent("Star Wars", List.of("A1", "A2", "B1", "B2"), producer);
            sendTicketSellEvent("Forrest Gump", List.of("C11", "C12"), producer);
            sendTicketSellEvent("Star Wars", List.of("C1", "C2", "C3", "C4"), producer);
            sendTicketSellEvent("Inception", List.of("I1", "I2"), producer);
            sendTicketSellEvent("Forrest Gump", List.of("D1", "D2"), producer);
            sendTicketSellEvent("Star Wars", List.of("F1", "F2", "F3", "F4"), producer);
            producer.flush();
        }

    }

    public void sendTicketSellEvent(String title, List<String> seats, KafkaProducer<String, TicketSellEvent> producer) {
        TicketSellEvent event = TicketSellEvent.newBuilder()
                .setMovieTitle(title)
                .setTicketCount(seats.size())
                .setSeats(seats)
                .build();
        producer.send(new ProducerRecord<>(TICKET_SELL_EVENT_TOPIC_NAME, title, event));

    }
}
