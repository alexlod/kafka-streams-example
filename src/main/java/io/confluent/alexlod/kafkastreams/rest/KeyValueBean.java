package io.confluent.alexlod.kafkastreams.rest;

import io.confluent.alexlod.kafkastreams.UrlRegionClicks;

import java.util.Objects;

public class KeyValueBean {

  private String key;
  private UrlRegionClicks value;

  public KeyValueBean() {}

  public KeyValueBean(String key, UrlRegionClicks value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public UrlRegionClicks getValue() {
    return value;
  }

  public void setValue(UrlRegionClicks value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KeyValueBean that = (KeyValueBean) o;
    return Objects.equals(key, that.key) &&
            Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value);
  }

  @Override
  public String toString() {
    return "KeyValueBean{" +
            "key='" + key + '\'' +
            ", value=" + value +
            '}';
  }
}
