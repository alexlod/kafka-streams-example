package io.confluent.alexlod.kafkastreams.rest;

import io.confluent.alexlod.kafkastreams.MyStreamJob;
import io.confluent.alexlod.kafkastreams.UrlRegionClicks;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * This is based on the WordCount example: https://github.com/confluentinc/examples/blob/3.1.x/kafka-streams/src/main/java/io/confluent/examples/streams/interactivequeries/WordCountInteractiveQueriesRestService.java
 */
@Path("/")
public class InteractiveQueriesRestService {
  private final KafkaStreams streams;
  private final MetadataService metadataService;
  private Server jettyServer;

  public InteractiveQueriesRestService(KafkaStreams streams) {
    this.streams = streams;
    this.metadataService = new MetadataService(streams);
  }

  @GET
  @Path("/")
  @Produces(MediaType.TEXT_HTML)
  public String index() {
    return "See MyStreamJob.java comments for example queries.";
  }

  @GET
  @Path("/region-clicks/{url}")
  @Produces(MediaType.APPLICATION_JSON)
  public KeyValueBean byKey(@PathParam("url") String url) {

    // Lookup the KeyValueStore with the provided storeName
    ReadOnlyKeyValueStore<String, UrlRegionClicks> store = streams.store(MyStreamJob.OUTPUT_STATE_STORE_NAME, QueryableStoreTypes.<String, UrlRegionClicks>keyValueStore());
    if (store == null) {
      throw new NotFoundException();
    }

    // Get the value from the store
    UrlRegionClicks value = store.get(url);
    if (value == null) {
      throw new NotFoundException();
    }
    return new KeyValueBean(url, value);
  }

  /**
   * Get the metadata for all of the instances of this Kafka Streams application
   */
  @GET()
  @Path("/instances")
  @Produces(MediaType.APPLICATION_JSON)
  public List<HostStoreInfo> streamsMetadata() {
    return metadataService.streamsMetadata();
  }

  /**
   * Find the metadata for the instance of this Kafka Streams Application that has the given
   * url if it exists.
   */
  @GET()
  @Path("/instances/{url}")
  @Produces(MediaType.APPLICATION_JSON)
  public HostStoreInfo streamsMetadataForStoreAndKey(@PathParam("url") String url) {
    return metadataService.streamsMetadataForStoreAndKey(MyStreamJob.OUTPUT_STATE_STORE_NAME, url, new StringSerializer());
  }

  public void start(int port) throws Exception {
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");

    jettyServer = new Server(port);
    jettyServer.setHandler(context);

    ResourceConfig rc = new ResourceConfig();
    rc.register(this);
    rc.register(JacksonFeature.class);

    ServletContainer sc = new ServletContainer(rc);
    ServletHolder holder = new ServletHolder(sc);
    context.addServlet(holder, "/*");

    jettyServer.start();
  }

  public void stop() throws Exception {
    if (jettyServer != null) {
      jettyServer.stop();
    }
  }
}
