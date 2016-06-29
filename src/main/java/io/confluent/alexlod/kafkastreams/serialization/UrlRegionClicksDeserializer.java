package io.confluent.alexlod.kafkastreams.serialization;

import io.confluent.alexlod.kafkastreams.UrlRegionClicks;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class UrlRegionClicksDeserializer implements Deserializer<UrlRegionClicks> {
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public UrlRegionClicks deserialize(String topic, byte[] data) {
    if (data == null || data.length == 0)
      return null;

    String strData;
    try {
      strData = new String(data, "UTF-8");
    } catch (UnsupportedEncodingException uee) {
      throw new RuntimeException("String could not be decoded with UTF-8.");
    }
    StringBuilder buffer = new StringBuilder();
    int delimeterCount = 0;
    Map<String, Long> regionClicks = new HashMap<String, Long>();
    String region = null;
    for (int i = 0; i < strData.length(); i++) {
      char c = strData.charAt(i);
      if (c == '|') {
        if (delimeterCount % 2 == 0)
          region = buffer.toString();
        else if (delimeterCount % 2 == 1) {
          long clicks = Long.parseLong(buffer.toString());
          regionClicks.put(region, clicks);
        }
        buffer = new StringBuilder();
        delimeterCount++;
      } else {
        buffer.append(c);
      }
    }
    String url = buffer.toString();

    UrlRegionClicks ret = new UrlRegionClicks(url);
    ret.setRegionClicks(regionClicks);
    return ret;
  }

  @Override
  public void close() {
  }
}
