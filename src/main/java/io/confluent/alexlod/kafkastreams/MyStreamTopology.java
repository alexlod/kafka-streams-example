package io.confluent.alexlod.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

/**
 * Reads clicks and locations. Calculates clicks for each URL broken down by region.
 * Produces this result to a topic.
 */
public class MyStreamTopology {
  public static void main (String[] args) {
    final Serde<String> stringSerde = Serdes.String();
    final Serde<Integer> intSerde = Serdes.Integer();

    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafkastreams-example-click-join");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
    streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, intSerde.getClass().getName());
    streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // TODO: finish this by reading:
    // http://docs.confluent.io/3.0.0/streams/quickstart.html
    // http://www.confluent.io/blog/distributed-real-time-joins-and-aggregations-on-user-activity-events-using-kafka-streams
    // https://github.com/confluentinc/examples/blob/master/kafka-streams/src/test/java/io/confluent/examples/streams/JoinLambdaIntegrationTest.java
  }
}
