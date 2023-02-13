package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.spi;

import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.IcebergSinkConnectorConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;

public interface IcebergWriterFactory {

  void configure(IcebergSinkConnectorConfig config);

  IcebergWriter create(Table table, Schema fileSchema, int partition, String operationId);
}
