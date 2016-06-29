package io.confluent.alexlod.kafkastreams.serialization;


import io.confluent.alexlod.kafkastreams.UrlRegionClicks;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class UrlRegionClicksSerializerTest {
  public static UrlRegionClicks makeUrlRegionClicks() {
    UrlRegionClicks ret = new UrlRegionClicks("http://foo.com");
    Map<String, Long> regionClicks = new HashMap<String, Long>();
    regionClicks.put("region1", (long)5);
    regionClicks.put("region2", (long)2);
    ret.setRegionClicks(regionClicks);
    return ret;
  }

  @Test
  public void testSerialize() throws Exception {
    Serializer<UrlRegionClicks> serializer = new UrlRegionClicksSerializer();
    UrlRegionClicks test = UrlRegionClicksSerializerTest.makeUrlRegionClicks();
    String serialized = new String(serializer.serialize(null, test), "UTF-8");

    assertEquals("Serialized output did not match expected output.", "region1|5|region2|2|http://foo.com", serialized);
  }
}
