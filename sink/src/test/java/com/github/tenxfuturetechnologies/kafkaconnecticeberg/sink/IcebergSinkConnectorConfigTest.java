package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.iceberg.DefaultIcebergWriterFactory;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.spi.IcebergWriter;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.spi.IcebergWriterFactory;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;

class IcebergSinkConnectorConfigTest {

  @Test
  void shouldReturnDefaultValues() {
    var props = new HashMap<String, String>();

    var config = new IcebergSinkConnectorConfig(props);

    assertThat(config.getTimezone()).isEqualTo(ZoneId.systemDefault());
    assertThat(config.getFlushSize()).isEqualTo(100);
    assertThat(config.getFlushWallClockRotateIntervalMs()).isEqualTo(MINUTES.toMillis(10));
    assertThat(config.getWriterFactoryClass()).isEqualTo(DefaultIcebergWriterFactory.class);
    assertThat(config.isWriteFileWithRecordSchema()).isFalse();
    assertThat(config.getSchemaCompatibility()).isEqualToIgnoringCase("NONE");
    assertThat(config.getTableNamespace()).isEqualTo("default");
    assertThat(config.getTableName()).isNull();
    assertThat(config.isTableAutoCreate()).isFalse();
    assertThat(config.isTableSchemaEvolve()).isFalse();
    assertThat(config.getTablePrimaryKey()).isEmpty();
    assertThat(config.getTablePartitionColumn()).isEmpty();
    assertThat(config.getTablePartitionTransform()).isEqualTo("void");
    assertThat(config.getCatalogName()).isEqualTo("default");
    assertThat(config.getIcebergConfiguration()).isEmpty();
  }

  @Test
  void shouldSetAllProperties() {
    var props = new HashMap<String, String>();
    props.put("timezone", "UTC");
    props.put("flush.size", "3");
    props.put("flush.wall-clock-rotate-interval-ms", "123");
    props.put("writer.factory.class", TestIcebergWriterFactory.class.getName());
    props.put("writer.keep-record-fields", "true");
    props.put("schema.compatibility", "full");
    props.put("table.namespace", "test_ns");
    props.put("table.name", "test_table");
    props.put("table.auto-create", "true");
    props.put("table.schema.evolve", "true");
    props.put("table.primary-key", "pkey");
    props.put("table.partition.column-name", "timestamp");
    props.put("table.partition.transform", "day");
    props.put("iceberg.name", "some_catalog");
    props.put("iceberg.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
    props.put("iceberg.warehouse", "s3a://my-bucket/iceberg");
    props.put("iceberg.fs.defaultFS", "s3a://my-bucket/iceberg");
    props.put("iceberg.com.amazonaws.services.s3.enableV4", "true");
    props.put("iceberg.com.amazonaws.services.s3a.enableV4", "true");
    props.put("iceberg.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
    props.put("iceberg.fs.s3a.path.style.access", "true");
    props.put("iceberg.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

    var config = new IcebergSinkConnectorConfig(props);

    assertThat(config.getTimezone()).isEqualTo(ZoneId.of("UTC"));
    assertThat(config.getFlushSize()).isEqualTo(3);
    assertThat(config.getFlushWallClockRotateIntervalMs()).isEqualTo(123);
    assertThat(config.getWriterFactoryClass()).isEqualTo(TestIcebergWriterFactory.class);
    assertThat(config.isWriteFileWithRecordSchema()).isTrue();
    assertThat(config.getSchemaCompatibility()).isEqualToIgnoringCase("FULL");
    assertThat(config.getTableNamespace()).isEqualTo("test_ns");
    assertThat(config.getTableName()).isEqualTo("test_table");
    assertThat(config.isTableAutoCreate()).isTrue();
    assertThat(config.isTableSchemaEvolve()).isTrue();
    assertThat(config.getTablePrimaryKey()).isEqualTo(Set.of("pkey"));
    assertThat(config.getTablePartitionColumn()).hasValue("timestamp");
    assertThat(config.getTablePartitionTransform()).isEqualTo("day");
    assertThat(config.getCatalogName()).isEqualTo("some_catalog");
    assertThat(config.getIcebergConfiguration())
        .hasSize(9)
        .containsEntry("name", "some_catalog")
        .containsEntry("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .containsEntry("warehouse", "s3a://my-bucket/iceberg")
        .containsEntry("fs.defaultFS", "s3a://my-bucket/iceberg")
        .containsEntry("com.amazonaws.services.s3.enableV4", "true")
        .containsEntry("com.amazonaws.services.s3a.enableV4", "true")
        .containsEntry("fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .containsEntry("fs.s3a.path.style.access", "true")
        .containsEntry("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
  }

  private static class TestIcebergWriterFactory implements IcebergWriterFactory {

    @Override
    public void configure(IcebergSinkConnectorConfig config) {
    }

    @Override
    public IcebergWriter create(
        Table table,
        Schema fileSchema,
        int partitionId,
        String operationId) {
      return null;
    }
  }
}