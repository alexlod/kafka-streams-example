package io.confluent.alexlod.kafkastreams.serialization;

import io.confluent.alexlod.kafkastreams.UrlRegionClicks;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class UrlRegionClicksSerializer implements Serializer<UrlRegionClicks> {
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public byte[] serialize(String topic, UrlRegionClicks data) {
    StringBuilder buffer = new StringBuilder();
    Map<String, Long> regionClicks = data.getRegionClicks();
    for (String region : regionClicks.keySet()) {
      buffer.append(region);
      buffer.append("|");
      buffer.append(regionClicks.get(region));
      buffer.append("|");
    }
    buffer.append(data.getUrl());
    return buffer.toString().getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void close() {
  }
}
