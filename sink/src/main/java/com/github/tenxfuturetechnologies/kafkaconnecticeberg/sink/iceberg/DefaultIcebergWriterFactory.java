package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.iceberg;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.IcebergSinkConnectorConfig;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.spi.IcebergWriter;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.spi.IcebergWriterFactory;
import com.google.common.primitives.Ints;
import java.util.Locale;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultIcebergWriterFactory implements IcebergWriterFactory {

  private static final Logger log = LoggerFactory.getLogger(DefaultIcebergWriterFactory.class);
  private static final long TASK_ID = 1L;

  @Override
  public void configure(IcebergSinkConnectorConfig config) {
    // NOOP
  }

  @Override
  public IcebergWriter create(Table table, Schema fileSchema, int partitionId, String operationId) {
    var format = getTableFileFormat(table);
    var appenderFactory = getTableAppender(table, fileSchema);
    var fileFactory = OutputFileFactory.builderFor(table, partitionId, TASK_ID)
        .defaultSpec(table.spec())
        .operationId(operationId)
        .format(format)
        .build();

    BaseTaskWriter<Record> writer;
    if (table.spec().isUnpartitioned()) {
      log.debug("Creating un-partitioned writer for table {}", table.name());
      writer = new UnpartitionedWriter<>(
          table.spec(),
          format,
          appenderFactory,
          fileFactory,
          table.io(),
          Long.MAX_VALUE);
    } else {
      log.debug("Creating partitioned appender for table {}", table.name());
      writer = new DefaultPartitionedFanoutWriter(
          table.spec(),
          format,
          appenderFactory,
          fileFactory,
          table.io(),
          Long.MAX_VALUE,
          table.schema());
    }

    return new DefaultIcebergWriter(table, writer);
  }

  private GenericAppenderFactory getTableAppender(Table table, Schema fileSchema) {
    return new GenericAppenderFactory(
        fileSchema,
        table.spec(),
        Ints.toArray(table.schema().identifierFieldIds()),
        fileSchema,
        null);
  }

  public FileFormat getTableFileFormat(Table table) {
    var formatAsString = table.properties()
        .getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    return FileFormat.valueOf(formatAsString.toUpperCase(Locale.ROOT));
  }
}
