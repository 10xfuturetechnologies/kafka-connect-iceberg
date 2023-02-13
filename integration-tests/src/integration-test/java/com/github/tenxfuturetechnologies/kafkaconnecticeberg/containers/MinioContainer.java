package com.github.tenxfuturetechnologies.kafkaconnecticeberg.containers;

import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.MINIO_IMAGE;
import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.S3_ACCESS_KEY;
import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.S3_REGION_NAME;
import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.S3_SECRET_KEY;

import java.time.Duration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

public class MinioContainer extends GenericContainer<MinioContainer> {

  private static final String NETWORK_ALIAS = "minio";
  private static final int MINIO_DEFAULT_PORT = 9000;
  private static final String DEFAULT_STORAGE_DIRECTORY = "/data";
  private static final String HEALTH_ENDPOINT = "/minio/health/ready";

  public MinioContainer() {
    super(MINIO_IMAGE);
    withNetworkAliases(NETWORK_ALIAS);
    withExposedPorts(MINIO_DEFAULT_PORT);
    withEnv("MINIO_ACCESS_KEY", S3_ACCESS_KEY);
    withEnv("MINIO_SECRET_KEY", S3_SECRET_KEY);
    withEnv("MINIO_REGION_NAME", S3_REGION_NAME);
    withCommand("server " + DEFAULT_STORAGE_DIRECTORY);
    waitingFor(new HttpWaitStrategy()
        .forPath(HEALTH_ENDPOINT)
        .forPort(MINIO_DEFAULT_PORT)
        .withStartupTimeout(Duration.ofSeconds(30)));
  }

  public String getInternalUrl() {
    return String.format("http://%s:%d", NETWORK_ALIAS, MINIO_DEFAULT_PORT);
  }

  public String getUrl() {
    return String.format("http://localhost:%d", getMappedPort(MINIO_DEFAULT_PORT));
  }
}
