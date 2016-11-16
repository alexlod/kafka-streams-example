package io.confluent.alexlod.kafkastreams;

import io.confluent.alexlod.kafkastreams.serialization.UrlRegionClicksSerde;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Reads clicks and locations. Calculates clicks for each URL broken down by region.
 * Produces this result to a topic.
 *
 * This example is similar, yet slightly, to this one:
 *   http://www.confluent.io/blog/distributed-real-time-joins-and-aggregations-on-user-activity-events-using-kafka-streams
 */
public class MyStreamJob {

  public static final String STREAMS_LOCAL_DIR = "/tmp/kafka-streams";

  public static final String OUTPUT_TOPIC = "url-region-clicks";

  private static final Logger log = LoggerFactory.getLogger(MyStreamJob.class);

  public static void main (String[] args) {
    final Serde<String> stringSerde = Serdes.String();
    final Serde<Integer> intSerde = Serdes.Integer();
    final Serde<UrlRegionClicks> regionUrlClicksSerde = new UrlRegionClicksSerde();

    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafkastreams-example-click-realtime-report");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
    streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, intSerde.getClass().getName());
    streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // Explicitly place the state directory under /tmp so that we can remove it via
    // `purgeLocalStreamsState` below.  Once Streams is updated to expose the effective
    // StreamsConfig configuration (so we can retrieve whatever state directory Streams came up
    // with automatically) we don't need to set this anymore and can update `purgeLocalStreamsState`
    // accordingly.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, STREAMS_LOCAL_DIR);
    deleteState();

    KStreamBuilder builder = new KStreamBuilder();
    KStream<Integer, String> clickStream = builder.stream(intSerde, stringSerde, ClickSimulator.CLICK_TOPIC);
    KTable<Integer, String> userLocationsTable = builder.table(intSerde, stringSerde, Bootstrapper.LOCATION_TOPIC, "LocationStateStore");

    // desired output: url -> (region1 -> 4, region2 -> 6, region3 -> 1)
    clickStream
            .leftJoin(userLocationsTable, (url, region) -> new UrlRegionClicks(url, region))
            .map((userId, regionUrlClicks) -> new KeyValue<String, UrlRegionClicks>(regionUrlClicks.getUrl(), regionUrlClicks))
            .groupByKey(stringSerde, regionUrlClicksSerde)
            .reduce((firstClicks, secondClicks) -> firstClicks.combine(secondClicks), "ClicksPerRegionUnwindowed") // the lambda could be replaced with `RegionUrlClicks::combine`.
            .to(stringSerde, regionUrlClicksSerde, OUTPUT_TOPIC);

    KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        streams.close();
        deleteState();
      }
    });
  }

  private static void deleteState() {
    try {
      FileUtils.deleteDirectory(new File(STREAMS_LOCAL_DIR));
    } catch (IOException ioe) {
      log.warn("Streams local directory couldn't be deleted. This may cause unexpected behavior. Directory: " +
              STREAMS_LOCAL_DIR);
    }
  }
}
