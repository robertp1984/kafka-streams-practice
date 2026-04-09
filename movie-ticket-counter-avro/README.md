1. Create the input topic which will be used to send the ticket sell events to the Kafka cluster.
```bash
kafka-topics --bootstrap-server kafka1:29092 --create --topic ticket.sellEvent --partitions 9 --replication-factor 3
```

2. Create the compact output topic which will be used to send the aggregated movie information to the Kafka cluster.
```bash
kafka-topics --bootstrap-server kafka1:29092 --create --topic ticket.soldSummary --partitions 9 --replication-factor 3 --config cleanup.policy=compact
```

3. Monitor the output topic to see the aggregated movie information.
```bash
kafka-avro-console-consumer --bootstrap-server kafka1:29092 --topic ticket.soldSummary --from-beginning --property print.key=false --property print.value=true --property schema.registry.url=http://schema-registry:8081
```

4. Run the TicketCounter application class from IDE to start the stream processing.

5. Run the TicketSellEventProducer application class from IDE to start sending the ticket sell events. The aggregated movie information should be printed in the console where you are monitoring the output topic.
