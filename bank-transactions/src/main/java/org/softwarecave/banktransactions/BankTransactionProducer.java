package org.softwarecave.banktransactions;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Properties;

@Slf4j
public class BankTransactionProducer {

    public static final String BANK_TRANSACTIONS_TOPIC_NAME = "bank.transactions";

    public static void main() throws InterruptedException {
        new BankTransactionProducer().run();
    }

    private boolean stopRequested = false;

    public void run() throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // The other settings are not needed as defaults are fine in recent Kafka versions

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Stop requested. Exiting.");
            this.stopRequested = true;
        }));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            while (!stopRequested) {
                sendRandomTransaction("John", producer);
                Thread.sleep(100);
                sendRandomTransaction("Jane", producer);
                Thread.sleep(100);
                sendRandomTransaction("Bob", producer);
                Thread.sleep(100);
                sendRandomTransaction("Alice", producer);
            }
        }
    }

    private static void sendRandomTransaction(String clientName, KafkaProducer<String, String> producer) {
        BankTransaction transaction = new BankTransaction(clientName,
                BigDecimal.valueOf(Math.random() * 100), LocalDateTime.now());

        JsonMapper mapper = new JsonMapper();
        String transactionJson = mapper.writeValueAsString(transaction);

        var producerRecord = new ProducerRecord<>(BANK_TRANSACTIONS_TOPIC_NAME, clientName, transactionJson);
        producer.send(producerRecord);
    }
}
