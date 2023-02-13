package com.github.tenxfuturetechnologies.kafkaconnecticeberg.containers;

import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.KAFKA_IMAGE;
import static java.lang.String.format;

import org.testcontainers.utility.DockerImageName;

public class KafkaContainer extends org.testcontainers.containers.KafkaContainer {

  private static final String NETWORK_ALIAS = "kafka";

  public KafkaContainer() {
    super(DockerImageName.parse(KAFKA_IMAGE));
    withNetworkAliases(NETWORK_ALIAS);
    withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true");
  }

  public String getInternalBootstrap() {
    return format("%s:9092", NETWORK_ALIAS);
  }
}
