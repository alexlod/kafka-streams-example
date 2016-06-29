package io.confluent.alexlod.kafkastreams.serialization;


import io.confluent.alexlod.kafkastreams.UrlRegionClicks;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UrlRegionClicksDeserializerTest {
  @Test
  public void testDeserialize() throws Exception {
    Serializer<UrlRegionClicks> serializer = new UrlRegionClicksSerializer();
    UrlRegionClicks test = UrlRegionClicksSerializerTest.makeUrlRegionClicks();
    String serialized = new String(serializer.serialize(null, test), "UTF-8");

    Deserializer<UrlRegionClicks> deserializer = new UrlRegionClicksDeserializer();
    UrlRegionClicks deserialized = deserializer.deserialize(null, serialized.getBytes());

    assertEquals("URL didn't match expected.", "http://foo.com", deserialized.getUrl());
    assertHashMapEquals(test.getRegionClicks(), deserialized.getRegionClicks());
  }

  private void assertHashMapEquals(Map<String, Long> one, Map<String, Long> two) {
    for (String region : one.keySet()) {
      assertEquals("Values didn't match for key: " + region, one.get(region), two.get(region));
    }
    for (String region : two.keySet()) {
      assertEquals("Values didn't match for key: " + region, two.get(region), one.get(region));
    }
  }
}
