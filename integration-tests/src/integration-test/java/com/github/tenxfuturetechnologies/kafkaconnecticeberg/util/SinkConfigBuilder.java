package com.github.tenxfuturetechnologies.kafkaconnecticeberg.util;

import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.S3_ACCESS_KEY;
import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.S3_BUCKET;
import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.S3_REGION_NAME;
import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.S3_SECRET_KEY;
import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.TABLE_NAME;
import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.TABLE_NAMESPACE;

import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.IcebergSinkConnectorConfig;
import java.util.HashMap;
import java.util.Map;

public final class SinkConfigBuilder {

  private SinkConfigBuilder() {
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private final Map<String, String> properties;

    private Builder() {
      properties = new HashMap<>();
      properties.put(IcebergSinkConnectorConfig.TIMEZONE_CONFIG, "UTC");
      properties.put(IcebergSinkConnectorConfig.FLUSH_SIZE_CONFIG, "1");
      properties.put(IcebergSinkConnectorConfig.TABLE_NAMESPACE_CONFIG, TABLE_NAMESPACE);
      properties.put(IcebergSinkConnectorConfig.TABLE_NAME_CONFIG, TABLE_NAME);
      properties.put(IcebergSinkConnectorConfig.TABLE_AUTO_CREATE_CONFIG, "true");
      properties.put(IcebergSinkConnectorConfig.TABLE_SCHEMA_EVOLVE_CONFIG, "false");
      properties.put(IcebergSinkConnectorConfig.CATALOG_NAME_CONFIG, "iceberg");
    }

    public Builder withProperty(String name, String value) {
      properties.put(name, value);
      return this;
    }

    public Builder withCustomCatalogProperty(String key, String value) {
      withProperty(IcebergSinkConnectorConfig.ICEBERG_PREFIX + key, value);
      return this;
    }

    public Builder withFlushSettings(int flushSize, long flushWallClockMs) {
      withProperty(IcebergSinkConnectorConfig.FLUSH_SIZE_CONFIG, Integer.toString(flushSize));
      withProperty(
          IcebergSinkConnectorConfig.FLUSH_WALL_CLOCK_ROTATE_INTERVAL_MS_CONFIG,
          Long.toString(flushWallClockMs));
      return this;
    }

    public Builder withIgnoreTombstones() {
      withProperty("transforms", "TombstoneHandler");
      withProperty(
          "transforms.TombstoneHandler.type",
          "io.confluent.connect.transforms.TombstoneHandler");
      withProperty("transforms.TombstoneHandler.behavior", "warn");
      return this;
    }

    public Builder withTableSchemaEvolve(boolean evolveSchema) {
      withProperty(
          IcebergSinkConnectorConfig.TABLE_SCHEMA_EVOLVE_CONFIG,
          Boolean.toString(evolveSchema));
      return this;
    }

    public Builder withWriterKeepRecordFields(boolean keepRecordFields) {
      withProperty(
          IcebergSinkConnectorConfig.WRITER_KEEP_RECORD_FIELDS_CONFIG,
          Boolean.toString(keepRecordFields));
      return this;
    }

    public Builder withS3(String s3Url) {
      withProperty(IcebergSinkConnectorConfig.CATALOG_TYPE_CONFIG, "hadoop");
      withCustomCatalogProperty("warehouse", "s3a://" + S3_BUCKET + "/iceberg_warehouse");
      withCustomCatalogProperty("fs.defaultFS", "s3a://" + S3_BUCKET);
      withCustomCatalogProperty("fs.s3a.endpoint.region", S3_REGION_NAME);
      withCustomCatalogProperty("fs.s3a.access.key", S3_ACCESS_KEY);
      withCustomCatalogProperty("fs.s3a.secret.key", S3_SECRET_KEY);
      withCustomCatalogProperty("fs.s3a.path.style.access", "true");
      withCustomCatalogProperty("fs.s3a.endpoint", s3Url);
      return this;
    }

    public IcebergSinkConnectorConfig build() {
      return new IcebergSinkConnectorConfig(properties);
    }
  }
}
