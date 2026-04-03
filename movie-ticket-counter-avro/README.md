1. Create the input topic which will be used to send the ticket sell events to the Kafka cluster.
```bash
kafka-topics --bootstrap-server kafka1:29092 --create --topic ticket-sell-event --partitions 3 --replication-factor 3
```

2. Create the compact output topic which will be used to send the aggregated movie information to the Kafka cluster.
```bash
kafka-topics --bootstrap-server kafka1:29092 --create --topic ticket-sold-summary --partitions 3 --replication-factor 3 --config cleanup.policy=compact
```

3. Monitor the output topic to see the aggregated movie information.
```bash
kafka-avro-console-consumer --bootstrap-server kafka1:29092 --topic ticket-sold-summary --from-beginning --property print.key=false --property print.value=true --property schema.registry.url=http://schema-registry:8081
```

4. Run the TicketCounter application class from IDE to start the stream processing

5. Run the kafka-avro-console-producer to produce the ticket sell events to the input topic.
```bash
export AVRO_SCHEMA='{
  "namespace": "org.softwarecave.movieticketcounter.avro",
  "type": "record",
  "name": "TicketSellEvent",
  "version": "1",
  "fields": [
    {
      "name": "movieTitle",
      "type": "string",
      "doc": "The title of the movie"
    },
    {
      "name": "ticketCount",
      "type": "int",
      "doc": "The number of tickets sold in this transaction"
    },
    {
      "name": "seats",
      "type": {
        "type": "array",
        "items": "string"
      },
      "doc": "The list of seat numbers sold in this transaction"
    }
  ]
}
'

kafka-avro-console-producer --bootstrap-server kafka1:29092 --topic ticket-sell-event --property value.schema="$AVRO_SCHEMA" --property schema.registry.url=http://schema-registry:8081
```

Then you can input the following JSON to send a ticket sell event to the Kafka cluster:`
```json 
{"movieTitle": "Awesome One", "ticketCount": 4, "seats": ["A1", "A2", "A3", "A4"]}
{"movieTitle": "Bad One", "ticketCount": 3, "seats": ["B1",  "B3", "B4"]}
{"movieTitle": "Awesome One", "ticketCount": 2, "seats": ["C1", "C4"]}
```

