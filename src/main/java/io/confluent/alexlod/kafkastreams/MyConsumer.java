package io.confluent.alexlod.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Helper consumer that prints messages in a topic for debugging purposes.
 */
public class MyConsumer {
  private static final Logger log = LoggerFactory.getLogger(MyStreamJob.class);

  public static void main (String[] args) {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "validator");
    props.put("auto.offset.reset", "earliest");
    props.put("enable.auto.commit", "false"); // will never commit offsets.
    props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    // first, consume from the click stream and locations topics.
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
            log.error("Received message from unknown topic. Ignoring. Topic: " + record.topic());
        }
      }
    } finally {
      consumer.close();
    }

    log.info("Clicks:");
    for (ConsumerRecord<Integer, String> click : clicks)
      log.info("\t" + click.key() + ": " + click.value());

    log.info("\nLocations:");
    for (ConsumerRecord<Integer, String> location : locations)
      log.info("\t" + location.key() + ": " + location.value());


    // TODO: these two poll loops could be combined into one method.

    // next, consume from the aggregated topic.
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "io.confluent.alexlod.kafkastreams.serialization.UrlRegionClicksDeserializer");

    KafkaConsumer<String, UrlRegionClicks> outputConsumer = new KafkaConsumer<String, UrlRegionClicks>(props);
    outputConsumer.subscribe(Arrays.asList(MyStreamJob.OUTPUT_TOPIC));

    log.info("\nUrl Region Clicks (KTable):");
    try {
      int consecutivePollsWithNoData = 0;
      while (consecutivePollsWithNoData < 10) {
        ConsumerRecords<String, UrlRegionClicks> records = outputConsumer.poll(500);
        if (records.isEmpty())
          consecutivePollsWithNoData++;
        else
          consecutivePollsWithNoData = 0;

        for (ConsumerRecord<String, UrlRegionClicks> record : records) {
          UrlRegionClicks value = record.value();
          log.info("\t" + value.getUrl() + ":");
          for (String region : value.getRegionClicks().keySet())
            log.info("\t\t" + region + ": " + value.getRegionClicks().get(region));
        }
      }
    } finally {
      outputConsumer.close();
    }
  }
}
