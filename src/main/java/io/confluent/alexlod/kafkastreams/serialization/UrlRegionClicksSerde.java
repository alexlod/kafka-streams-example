package io.confluent.alexlod.kafkastreams.serialization;

import io.confluent.alexlod.kafkastreams.UrlRegionClicks;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class UrlRegionClicksSerde implements Serde<UrlRegionClicks> {
  final private Serializer<UrlRegionClicks> serializer;
  final private Deserializer<UrlRegionClicks> deserializer;

  public UrlRegionClicksSerde() {
    this.serializer = new UrlRegionClicksSerializer();
    this.deserializer = new UrlRegionClicksDeserializer();
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.serializer.configure(configs, isKey);
    this.deserializer.configure(configs, isKey);
  }

  @Override
  public void close() {
    this.serializer.close();
    this.deserializer.close();
  }

  @Override
  public Serializer<UrlRegionClicks> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<UrlRegionClicks> deserializer() {
    return deserializer;
  }
}
