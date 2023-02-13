package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink;

import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.converter.RecordConverter;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.converter.SchemaConverter;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.iceberg.IcebergCatalogFactory;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.iceberg.IcebergTableManager;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpecFactory;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.spi.IcebergWriterFactory;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.util.Version;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(IcebergSinkTask.class);

  private IcebergSinkConnectorConfig config;
  private IcebergWriterFactory icebergWriterFactory;
  private IcebergTableManager tableManager;
  private RecordConverter recordConverter;
  private SchemaConverter schemaConverter;
  private RecordWriter recordWriter;
  private ErrantRecordReporter reporter;

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) {
    log.info("Task starting");
    config = new IcebergSinkConnectorConfig(properties);
    recordConverter = new RecordConverter(config);
    schemaConverter = new SchemaConverter(config);
    var partitionTransformSpecFactory = new PartitionTransformSpecFactory();
    tableManager = new IcebergTableManager(
        config,
        IcebergCatalogFactory.create(config),
        partitionTransformSpecFactory);
    icebergWriterFactory = createIcebergWriterFactory();
    try {
      reporter = context.errantRecordReporter();
      log.info("Errant record reporter configured? {}", (reporter != null));
    } catch (NoSuchMethodError | NoClassDefFoundError | UnsupportedOperationException e) {
      log.warn("Errant record reporter not supported");
    }
    if (recordWriter == null) {
      openWriter();
    }
    log.info("Started Iceberg sink connector task");
  }


  @Override
  public void put(Collection<SinkRecord> records) {
    log.debug("Received {} records", records.size());
    records.stream().forEach(recordWriter::buffer);

    try {
      log.debug("About to flush");
      recordWriter.flush();
    } catch (RuntimeException e) {
      log.error("Exception writing records", e);
      // TODO reset consumer and restart rather than rethrowing
      throw e;
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    var committableOffsets = recordWriter.getCommittableOffsets()
        .entrySet()
        .stream()
        .filter(entry -> currentOffsets.containsKey(entry.getKey()))
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    log.info("Committing offsets {}", committableOffsets);
    return committableOffsets;
  }

  @Override
  public void stop() {
    closeWriter();
    log.info("Task stopped");
  }

  private void openWriter() {
    recordWriter = new RecordWriter(
            context,
            config,
            tableManager,
            recordConverter,
            schemaConverter,
            icebergWriterFactory,
            reporter);
    log.info("Iceberg record writer is now ready");
  }

  private void closeWriter() {
    if (recordWriter != null) {
      recordWriter.close();
      recordWriter = null;
      log.info("Iceberg record writer is now closed");
    }
  }

  private IcebergWriterFactory createIcebergWriterFactory() {
    IcebergWriterFactory factory = null;
    try {
      factory = config.getWriterFactoryClass().getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new ConfigException("Unable to create IcebergWriter factory", e);
    }
    factory.configure(config);
    return factory;
  }
}
