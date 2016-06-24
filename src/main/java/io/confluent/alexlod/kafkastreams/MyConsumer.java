package io.confluent.alexlod.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Helper consumer that prints messages in a topic for debugging purposes.
 */
public class MyConsumer {
  public static void main (String[] args) {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "validator");
    props.put("auto.offset.reset", "earliest");
    props.put("enable.auto.commit", "false"); // will never commit offsets.
    props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    KafkaConsumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(props);

    consumer.subscribe(Arrays.asList(MyProducer.CLICK_TOPIC, MyProducer.LOCATION_TOPIC));

    List<ConsumerRecord<Integer, String>> clicks = new LinkedList<ConsumerRecord<Integer, String>>();
    List<ConsumerRecord<Integer, String>> locations = new LinkedList<ConsumerRecord<Integer, String>>();
    try {
      int consecutivePollsWithNoData = 0;
      while (consecutivePollsWithNoData < 10) {
        ConsumerRecords<Integer, String> records = consumer.poll(500);
        if (records.isEmpty())
          consecutivePollsWithNoData++;
        else
          consecutivePollsWithNoData = 0;

        for (ConsumerRecord<Integer, String> record : records) {
          if (record.topic().equals(MyProducer.CLICK_TOPIC))
            clicks.add(record);
          else if (record.topic().equals(MyProducer.LOCATION_TOPIC))
            locations.add(record);
          else
            System.out.println("Received message from unknown topic. Ignoring. Topic: " + record.topic());
        }
      }

      System.out.println("Clicks:");
      for (ConsumerRecord<Integer, String> click : clicks)
        System.out.println("\t" + click.key() + ": " + click.value());

      System.out.println("\nLocations:");
      for (ConsumerRecord<Integer, String> location : locations)
        System.out.println("\t" + location.key() + ": " + location.value());
    } finally {
      consumer.close();
    }
  }
}
