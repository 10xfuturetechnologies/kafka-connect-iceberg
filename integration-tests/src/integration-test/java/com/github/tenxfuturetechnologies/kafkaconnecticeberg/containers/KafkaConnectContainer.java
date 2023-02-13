package com.github.tenxfuturetechnologies.kafkaconnecticeberg.containers;

import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.KAFKA_CONNECT_IMAGE;
import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.S3_ACCESS_KEY;
import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.S3_SECRET_KEY;
import static java.lang.String.format;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

public class KafkaConnectContainer extends GenericContainer<KafkaConnectContainer> {

  private static final String NETWORK_ALIAS = "connect";
  private static final int PORT = 8083;
  private static final String ADDITIONAL_PLUGIN_PATH = "/kafka-connect/";
  private static final String ZIP_FILE_NAME = "kafka-connect-iceberg-sink.zip";
  private static final String PLUGIN_ZIP_PATH =
      format("%s%s", ADDITIONAL_PLUGIN_PATH, ZIP_FILE_NAME);
  private static final String DISTRIBUTIONS_PATH = "./build/deps/distributions/";


  public KafkaConnectContainer() {
    super(new ImageFromDockerfile()
        .withFileFromPath(ZIP_FILE_NAME, pluginPath(ZIP_FILE_NAME))
        .withDockerfileFromBuilder(
            builder -> builder.from(KAFKA_CONNECT_IMAGE)
                .copy(ZIP_FILE_NAME, PLUGIN_ZIP_PATH)
                .run(format("confluent-hub install --no-prompt %s", PLUGIN_ZIP_PATH))
                .build()));
    withNetworkAliases(NETWORK_ALIAS);
    withExposedPorts(PORT);
    withEnv("CONNECT_GROUP_ID", "default");
    withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "default.offsets");
    withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "default.config");
    withEnv("CONNECT_STATUS_STORAGE_TOPIC", "default.status");
    withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1");
    withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1");
    withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1");
    withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
    withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
    withEnv("CONNECT_INTERNAL_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
    withEnv("CONNECT_INTERNAL_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
    withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "kafka-connect");
    withEnv("CONNECT_REST_PORT", String.valueOf(PORT));
    withEnv("AWS_ACCESS_KEY_ID", S3_ACCESS_KEY);
    withEnv("AWS_SECRET_KEY", S3_SECRET_KEY);
    withEnv("CONNECT_LOG4J_LOGGERS", "com.tenx.analytics.kafkaconnecticeberg.sink=DEBUG");
    waitingFor(Wait.forHttp("/").withStartupTimeout(Duration.ofSeconds(120L)));
  }

  private static Path pluginPath(String fileName) {
    var pkg = new File(format("%s%s", DISTRIBUTIONS_PATH, fileName));
    if (!pkg.exists()) {
      throw new IllegalArgumentException("File " + pkg.getAbsolutePath() + " does not exist");
    }
    return pkg.toPath();
  }

  public KafkaConnectContainer withKafkaBootstrap(String kafkaBootstrap) {
    return withEnv("CONNECT_BOOTSTRAP_SERVERS", kafkaBootstrap);
  }

  public String getUrl() {
    return format("http://localhost:%d", getMappedPort(PORT));
  }
}
