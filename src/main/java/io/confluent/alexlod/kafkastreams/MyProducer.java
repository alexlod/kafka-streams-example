package io.confluent.alexlod.kafkastreams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * Produce messages to two topics:
 *
 * clicks:
 *   key: user_id (int)
 *   value: url (string)
 *
 * locations:
 *   key: user_id (int)
 *   value: url (string)
 */
public class MyProducer {
  public static final int CLICK_MESSAGES = 200;
  public static final int USER_COUNT = 10;

  public static final String CLICK_TOPIC = "clicks";
  public static final String LOCATION_TOPIC = "locations";

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 5);
    props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Random randomGenerator = new Random();

    Producer<Integer, String> producer = new KafkaProducer<Integer, String>(props);

    try {
      // produce click data.
      for (int i = 0; i < CLICK_MESSAGES; i++) {
        int userId = randomGenerator.nextInt(USER_COUNT);
        String url = "http://foo.bar/" + randomGenerator.nextInt(6);
        producer.send(new ProducerRecord<Integer, String>(CLICK_TOPIC, userId, url));
      }

      // produce location data.
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
