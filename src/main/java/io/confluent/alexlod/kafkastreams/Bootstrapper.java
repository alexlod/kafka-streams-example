package io.confluent.alexlod.kafkastreams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * Produce user profile information into the following topic:
 *
 * locations:
 *   key: user_id (int)
 *   value: url (string)
 *
 */
public class Bootstrapper {
  public static final int USER_COUNT = 10;

  public static final String LOCATION_TOPIC = "locations";

  public static Producer<Integer, String> buildProducer() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 5);
    props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    return new KafkaProducer<Integer, String>(props);
  }

  public static void main(String[] args) {
    Random randomGenerator = new Random();

    Producer<Integer, String> producer = buildProducer();

    try {
      // produce location data. These must be produced before the click data so they're loaded
      // into the KTable before the KStream arrives with clicks.
      for (int userId = 0; userId < USER_COUNT; userId++) {
        int random = randomGenerator.nextInt(3);
        String location;
        switch (random) {
          case 0:
            location = "San Francisco";
            break;
          case 1:
            location = "Palo Alto";
            break;
          default:
            location = "Woodside";
        }
        producer.send(new ProducerRecord<Integer, String>(LOCATION_TOPIC, userId, location));
      }
    } finally {
      producer.close();
    }
  }
}
