package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.spi;

import org.apache.iceberg.data.Record;

public interface IcebergWriter {

  void write(Record record);

  void complete();

  boolean isComplete();

  void close();
}
