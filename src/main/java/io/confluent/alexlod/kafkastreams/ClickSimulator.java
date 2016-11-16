package io.confluent.alexlod.kafkastreams;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;

/**
 * Simulate clicks by periodically producing clicks into the following topic:
 *
 *  * clicks:
 *   key: user_id (int)
 *   value: url (string)
 *
 */
public class ClickSimulator {
  public static final String CLICK_TOPIC = "clicks";

  public static final long DELAY_BETWEEN_CLICKS_MS = 10000;

  public static void main(String[] args) {
    Random randomGenerator = new Random();

    Producer<Integer, String> producer = Bootstrapper.buildProducer();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        producer.close();
      }
    });

    // produce click data.
    while(true) {
      int userId = randomGenerator.nextInt(Bootstrapper.USER_COUNT);
      String url = "http://foo.bar/" + randomGenerator.nextInt(6);
      producer.send(new ProducerRecord<Integer, String>(CLICK_TOPIC, userId, url));
      try {
        Thread.sleep(DELAY_BETWEEN_CLICKS_MS);
      } catch (InterruptedException ie) {
        // do nothing.
      }
    }
  }
}
