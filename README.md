A simple sandbox/example I built that uses KafkaStreams. This example is similar to:


  https://github.com/confluentinc/examples/blob/master/kafka-streams/src/test/java/io/confluent/examples/streams/JoinLambdaIntegrationTest.java

Follow these steps to run this:

 * Run ZooKeeper locally on `localhost:2181`
 * Run Kafka locally on `localhost:9092` with default settings (in particular, `auto.create.topics.enable=true`)
 * Run MyProducer.java
 * Run MyConsumer.java to see the state in Kafka
 * Run MyStreamJob.java to run the stream job
 * Run MyConsumer.java to see both the starting state, and the state created by the stream job.
