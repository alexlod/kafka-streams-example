package io.confluent.alexlod.kafkastreams.rest;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.StreamsMetadata;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.NotFoundException;

/**
 * Looks up StreamsMetadata from KafkaStreams and converts the results
 * into Beans that can be JSON serialized via Jersey.
 */
public class MetadataService {

  private final KafkaStreams streams;

  public MetadataService(KafkaStreams streams) {
    this.streams = streams;
  }

  /**
   * Get the metadata for all of the instances of this Kafka Streams application
   */
  public List<HostStoreInfo> streamsMetadata() {
    return mapInstancesToHostStoreInfo(streams.allMetadata());
  }

  /**
   * Find the metadata for the instance of this Kafka Streams Application that has the given
   * store and would have the given key if it exists.
   */
  public <K> HostStoreInfo streamsMetadataForStoreAndKey(String store, K key, Serializer<K> serializer) {
    StreamsMetadata metadata = streams.metadataForKey(store, key, serializer);
    if (metadata == null) {
      throw new NotFoundException();
    }

    return new HostStoreInfo(metadata.host(),
                             metadata.port(),
                             metadata.stateStoreNames());
  }

  private List<HostStoreInfo> mapInstancesToHostStoreInfo(Collection<StreamsMetadata> metadatas) {
    return metadatas.stream().map(metadata -> new HostStoreInfo(metadata.host(),
                                                                metadata.port(),
                                                                metadata.stateStoreNames()))
                             .collect(Collectors.toList());
  }

}
