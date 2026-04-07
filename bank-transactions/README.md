1. Create the input topic which will be used to send the bank transactions to the Kafka cluster.
```bash
kafka-topics --bootstrap-server kafka1:29092 --create --topic bank.transactions --partitions 3 --replication-factor 3
```

2. Create the compact output topic which will be used to send the aggregated transactions to the Kafka cluster.
```bash
kafka-topics --bootstrap-server kafka1:29092 --create --topic bank.transactions.aggregated --partitions 3 --replication-factor 3 --config cleanup.policy=compact
```

3. Monitor the output topic to see the aggregated transactions.
```bash
kafka-console-consumer --bootstrap-server kafka1:29092 --topic bank.transactions.aggregated --from-beginning --property print.key=true --property print.value=true
```

4. Run the Kafka Producer application class BankTransactionProducer from IDE to send the random bank transactions to the input topic. 


5. Run the Kafka Streams application class BankTransactionAggregator or BankTransactionAggregator2 from IDE to start the stream processing and see the aggregated transactions in the output topic.
