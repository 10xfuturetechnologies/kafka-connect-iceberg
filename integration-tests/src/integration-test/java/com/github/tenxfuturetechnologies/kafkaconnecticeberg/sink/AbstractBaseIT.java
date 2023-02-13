package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.containers.KafkaConnectContainer;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.containers.KafkaContainer;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.containers.MinioContainer;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.containers.SchemaRegistryContainer;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.MinioTestHelper;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.SparkTestHelper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public abstract class AbstractBaseIT {

  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  protected static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

  protected static final Network network = Network.newNetwork();

  protected static final @Container MinioContainer MINIO_CONTAINER =
      new MinioContainer()
          .withNetwork(network);

  protected static final @Container KafkaContainer KAFKA_CONTAINER =
      (KafkaContainer) new KafkaContainer()
          .withNetwork(network);

  protected static final @Container SchemaRegistryContainer SCHEMA_REGISTRY_CONTAINER =
      new SchemaRegistryContainer()
          .withNetwork(network)
          .withKafkaBoostrap(KAFKA_CONTAINER.getInternalBootstrap())
          .dependsOn(KAFKA_CONTAINER);

  protected static final @Container KafkaConnectContainer KAFKA_CONNECT_CONTAINER =
      new KafkaConnectContainer().withNetwork(network)
          .withKafkaBootstrap(KAFKA_CONTAINER.getInternalBootstrap())
          .dependsOn(KAFKA_CONTAINER, SCHEMA_REGISTRY_CONTAINER, MINIO_CONTAINER);

  private static final String RUNNING_STATE = "RUNNING";


  private static SparkTestHelper sparkTestHelper;
  private static MinioTestHelper minioTestHelper;

  @BeforeAll
  static void setup() {
    sparkTestHelper = new SparkTestHelper(MINIO_CONTAINER.getUrl());
    minioTestHelper = new MinioTestHelper(MINIO_CONTAINER.getUrl());
    minioTestHelper.createDefaultBucket();
  }

  protected static SparkTestHelper getSparkTestHelper() {
    return sparkTestHelper;
  }

  protected static MinioTestHelper getMinioTestHelper() {
    return minioTestHelper;
  }

  protected static void createConnector(String jsonConfig) {
    var request = HttpRequest.newBuilder()
        .uri(URI.create(KAFKA_CONNECT_CONTAINER.getUrl() + "/connectors/"))
        .header("Content-type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(jsonConfig))
        .build();
    HttpResponse<String> response;
    try {
      response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (Exception e) {
      throw new RuntimeException("Unable to create connector", e);
    }

    assertThat(response.statusCode()).isEqualTo(201);
  }

  protected static void deleteConnector(String connectorName) {
    var request = HttpRequest.newBuilder()
        .uri(URI.create(KAFKA_CONNECT_CONTAINER.getUrl() + "/connectors/" + connectorName))
        .header("Content-type", "application/json")
        .DELETE()
        .build();
    HttpResponse<String> response;
    try {
      response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (Exception e) {
      throw new RuntimeException("Unable to delete connector", e);
    }

    assertThat(response.statusCode()).isEqualTo(204);
  }

  protected static Map<String, Object> connectorStatus(String connectorName) {
    var url = KAFKA_CONNECT_CONTAINER.getUrl() + "/connectors/" + connectorName + "/status";
    var request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .header("Content-type", "application/json")
        .GET()
        .build();
    HttpResponse<String> response;
    try {
      response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (Exception e) {
      throw new RuntimeException("Unable to delete connector", e);
    }

    assertThat(response.statusCode()).isEqualTo(200);
    try {
      return OBJECT_MAPPER.readValue(response.body(), Map.class);
    } catch (Exception e) {
      throw new RuntimeException("Unable to deserialise connector status", e);
    }
  }

  protected static void waitTillConnectorIsRunning(String connectorName, Duration timeout) {
    await()
        .ignoreExceptions()
        .atMost(timeout)
        .until(() -> {
          var status = connectorStatus(connectorName);
          return isConnectorInState(status, RUNNING_STATE)
              && areTasksInState(status, RUNNING_STATE);
        });
    try {
      Thread.sleep(500L);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static boolean isConnectorInState(Map<String, Object> connectorStatus, String state) {
    return ((Map) connectorStatus.get("connector")).get("state").equals(state);
  }

  private static boolean areTasksInState(Map<String, Object> connectorStatus, String state) {
    var tasks = ((List) connectorStatus.get("tasks"));
    return tasks.size() > 0 && tasks
        .stream()
        .map(m -> ((Map) m).get("state"))
        .allMatch(s -> s.equals(state));
  }

  protected <T extends GenericRecord> ProducerRecord<String, T> createProducerRecord(
      T event,
      Function<T, ?> partitionExtractor,
      String topic) {
    return new ProducerRecord<>(topic, partitionExtractor.apply(event).toString(), event);
  }

  protected <T> KafkaProducer<String, T> createProducer(
      Class<? extends Serializer> keySerialiser,
      Class<? extends Serializer> valSerialiser) {
    var props = new Properties();
    props.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", keySerialiser.getName());
    props.put("value.serializer", valSerialiser.getName());
    props.put("schema.registry.url", SCHEMA_REGISTRY_CONTAINER.getUrl());
    return new KafkaProducer<>(props);
  }
}
