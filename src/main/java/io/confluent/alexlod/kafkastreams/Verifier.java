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
 * Helper consumer that prints messages in a topic for debugging purposes for the purposes of verification.
 */
public class Verifier {
  private static final Logger log = LoggerFactory.getLogger(MyStreamJob.class);

  public static void main (String[] args) {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "validator");
    props.put("auto.offset.reset", "earliest");
    props.put("enable.auto.commit", "false"); // will never commit offsets.
    props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    // first, consume from the locations topics. Because it will never grow, stop consuming
    // when all records have been consumed.
    KafkaConsumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(props);
    consumer.subscribe(Arrays.asList(Bootstrapper.LOCATION_TOPIC));

    List<ConsumerRecord<Integer, String>> locations = new LinkedList<ConsumerRecord<Integer, String>>();
    try {
      int consecutivePollsWithNoData = 0;
      log.info("\nLocations:");
      while (consecutivePollsWithNoData < 10) {
        ConsumerRecords<Integer, String> records = consumer.poll(500);
        if (records.isEmpty())
          consecutivePollsWithNoData++;
        else
          consecutivePollsWithNoData = 0;

        for (ConsumerRecord<Integer, String> record : records) {
          log.info("\t" + record.key() + ": " + record.value());
        }
      }
    } finally {
      consumer.close();
    }

    // next, consume from the aggregated topic, and consume forever.
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "io.confluent.alexlod.kafkastreams.serialization.UrlRegionClicksDeserializer");

    KafkaConsumer<String, UrlRegionClicks> outputConsumer = new KafkaConsumer<String, UrlRegionClicks>(props);
    outputConsumer.subscribe(Arrays.asList(MyStreamJob.OUTPUT_TOPIC));

    log.info("\nUrl Region Clicks (KTable):");
    try {
      while (true) {
        ConsumerRecords<String, UrlRegionClicks> records = outputConsumer.poll(500);

        for (ConsumerRecord<String, UrlRegionClicks> record : records) {
          UrlRegionClicks value = record.value();
          log.info("\t" + value.getUrl() + ":");
          for (String region : value.getRegionClicks().keySet())
            log.info("\t\t" + region + ": " + value.getRegionClicks().get(region));
        }
      }
    } finally {
      // TODO: add a shutdown hook.
      outputConsumer.close();
    }
  }
}
