package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink;

import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.util.Version;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSinkConnector extends SinkConnector {

  private static final Logger log = LoggerFactory.getLogger(IcebergSinkConnector.class);
  private Map<String, String> properties;
  private IcebergSinkConnectorConfig config;

  @Override
  public void start(Map<String, String> properties) {
    // TODO remove these 2 lines
    log.info("Sink configuration:");
    properties.forEach((key, value) -> log.info(String.format("%s : %s", key, value)));
    this.properties = new HashMap<>(properties);
    config = new IcebergSinkConnectorConfig(properties);
    log.info("Starting Iceberg sink connector {}", config.getName());
  }

  @Override
  public Class<? extends Task> taskClass() {
    return IcebergSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return IntStream.range(0, maxTasks).mapToObj(i -> properties).collect(Collectors.toList());
  }

  @Override
  public void stop() {
    log.info("Shutting down Iceberg sink connector {}", config.getName());
  }

  @Override
  public ConfigDef config() {
    return IcebergSinkConnectorConfig.newConfigDef();
  }

  @Override
  public String version() {
    return Version.getVersion();
  }
}
