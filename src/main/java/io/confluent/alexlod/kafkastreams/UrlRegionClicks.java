package io.confluent.alexlod.kafkastreams;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public  class UrlRegionClicks {

  private final String url;
  private  Map<String, Long> regionClicks;

  private static final Logger log = LoggerFactory.getLogger(UrlRegionClicks.class);

  public UrlRegionClicks(String url) {
    if (url == null || url.equals(""))
      throw new IllegalArgumentException("url must be set");
    this.url = url;
    this.regionClicks = new HashMap<String, Long>();
  }

  public UrlRegionClicks (String url, String region) {
    this(url);

    String myRegion;
    if (region == null || region.equals("")) {
      log.warn("Unknown region, using default `UNKNOWN`: " + region);
      myRegion = "UNKNOWN";
    } else
      myRegion = region;
    this.regionClicks.put(myRegion, (long)1);
  }

  public String getUrl() { return this.url; }

  public Map<String, Long> getRegionClicks() { return this.regionClicks; }

  // for testing.
  public void setRegionClicks(Map<String, Long> map) {
    this.regionClicks = map;
  }

  public UrlRegionClicks combine(UrlRegionClicks other) {
    if (!other.url.equals(this.url))
      throw new IllegalArgumentException("Can only combine when both the URLs are equal!");

    UrlRegionClicks ret = new UrlRegionClicks(this.url);

    Set<String> regions = new HashSet<String>();
    regions.addAll(this.regionClicks.keySet());
    regions.addAll(other.regionClicks.keySet());

    for (String region : regions) {
      Long thisCount = this.regionClicks.get(region);
      Long otherCount = other.regionClicks.get(region);
      long sum;
      if (thisCount != null && otherCount != null)
        sum = thisCount + otherCount;
      else if (thisCount != null)
        sum = thisCount;
      else if (otherCount != null)
        sum = otherCount;
      else
        sum = 0;
      ret.regionClicks.put(region, sum);
    }

    return ret;
  }
}
