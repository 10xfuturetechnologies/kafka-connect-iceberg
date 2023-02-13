package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink;

import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.converter.RecordConverter;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.converter.SchemaConverter;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.iceberg.IcebergTableManager;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.schema.RecordSchemaCompatibilityTracker;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.spi.IcebergWriter;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.spi.IcebergWriterFactory;
import java.io.Closeable;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordWriter implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(RecordWriter.class);

  // TODO get rid of this once we have on writer per TopicPartition
  private static final DateTimeFormatter FILE_PREFIX_FORMATTER =
      DateTimeFormatter.ofPattern("yyyyMMdd")
          .withZone(ZoneOffset.UTC);

  private final SinkTaskContext context;
  private final IcebergSinkConnectorConfig config;
  private final IcebergTableManager tableManager;
  private final RecordConverter recordConverter;
  private final SchemaConverter schemaConverter;
  private final IcebergWriterFactory icebergWriterFactory;
  private final ErrantRecordReporter reporter;
  private final RecordSchemaCompatibilityTracker schemaCompatibilityTracker;
  private final Map<TopicPartition, OffsetAndMetadata> currentOffsets;
  private final Map<TopicPartition, OffsetAndMetadata> committableOffsets;
  private final int flushSize;
  private Clock wallClock;
  private Queue<SinkRecord> buffer;
  private IcebergWriter icebergWriter;
  private long recordCount;
  private long wallClockRotateIntervalMs;
  private long lastCommitTimestamp;
  private Schema fileSchema;

  public RecordWriter(
      SinkTaskContext context,
      IcebergSinkConnectorConfig config,
      IcebergTableManager tableManager,
      RecordConverter recordConverter,
      SchemaConverter schemaConverter,
      IcebergWriterFactory icebergWriterFactory,
      ErrantRecordReporter reporter) {
    this.context = context;
    this.config = config;
    this.tableManager = tableManager;
    this.recordConverter = recordConverter;
    this.schemaConverter = schemaConverter;
    this.icebergWriterFactory = icebergWriterFactory;
    this.reporter = reporter;
    schemaCompatibilityTracker = new RecordSchemaCompatibilityTracker(config);
    currentOffsets = new HashMap<>();
    committableOffsets = new HashMap<>();
    wallClock = Clock.system(config.getTimezone());
    flushSize = config.getFlushSize();
    wallClockRotateIntervalMs = config.getFlushWallClockRotateIntervalMs();
    buffer = new LinkedList<>();
    lastCommitTimestamp = wallClock.millis();
  }

  public void buffer(SinkRecord sinkRecord) {
    if (sinkRecord.value() == null) {
      throw new DataException("Tombstone records cannot be handle by this task");
    }
    buffer.add(sinkRecord);
  }

  public Map<TopicPartition, OffsetAndMetadata> getCommittableOffsets() {
    return committableOffsets;
  }

  public void flush() {
    pause();

    while (!buffer.isEmpty()) {
      commitIfNeeded();

      try {
        var sinkRecord = buffer.peek();
        log.debug(
            "Processing record from topic {}, partition {} and offset {}",
            sinkRecord.topic(),
            sinkRecord.kafkaPartition(),
            sinkRecord.kafkaOffset()
        );
        if (shouldRotateOnSchema(sinkRecord)) {
          // Force file commit
          commit();
          schemaCompatibilityTracker.updateSchema(sinkRecord);
        } else {
          writeRecord(sinkRecord);
        }
      } catch (IllegalWorkerStateException e) {
        throw new ConnectException(e);
      } catch (SchemaProjectorException e) {
        if (!report(buffer.poll(), e)) {
          throw e;
        }
      }
    }

    commitIfNeeded();
    resume();
  }

  @Override
  public void close() {
    if (icebergWriter != null && !icebergWriter.isComplete()) {
      icebergWriter.close();
    }
  }

  private void writeRecord(SinkRecord sinkRecord) {
    var projectedRecord = schemaCompatibilityTracker.project(sinkRecord);
    var recordSchema = schemaConverter.convert(projectedRecord.valueSchema());
    var table = tableManager.from(recordSchema);
    if (icebergWriter == null || icebergWriter.isComplete()) {
      // TODO in order to have repeatable filenames we need to set both task and operation IDs
      // TODO to something predictable, e.g. partition and topic+offset. For this we need to create
      // TODO one record writer per TopicPartition
      var partitionId = Integer.parseInt(FILE_PREFIX_FORMATTER.format(Instant.now()));
      var operationId = UUID.randomUUID().toString();
      // TODO this is a patch until we understand why Iceberg throws an exception if we read
      // TODO files written with a schema dissociated from the table
      fileSchema = config.isWriteFileWithRecordSchema()
          ? tableManager.mergeTableSchema(recordSchema)
          : table.schema();
      icebergWriter = icebergWriterFactory.create(table, fileSchema, partitionId, operationId);
    }
    var icebergRecord = recordConverter.convert(projectedRecord, fileSchema);
    icebergWriter.write(icebergRecord);
    ++recordCount;
    buffer.poll();
    updateCurrentOffsets(sinkRecord);
  }

  private void updateCurrentOffsets(SinkRecord sinkRecord) {
    currentOffsets.put(
        new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition()),
        new OffsetAndMetadata(sinkRecord.kafkaOffset()));
  }

  private TopicPartition[] assignment() {
    return context.assignment().toArray(new TopicPartition[0]);
  }

  private void pause() {
    log.debug("Pausing assignment");
    context.pause(assignment());
  }

  private void resume() {
    log.debug("Resuming assignment");
    context.resume(assignment());
  }

  private void commitIfNeeded() {
    if (shouldRotateOnTime() || shouldRotateOnSize()) {
      log.info("Starting file commit");
      commit();
    }
  }

  private boolean shouldRotateOnSchema(SinkRecord sinkRecord) {
    var incompatibleSchema = schemaCompatibilityTracker.isIncompatible(sinkRecord);
    var schemaRotation = recordCount > 0 && incompatibleSchema;
    log.trace(
        "Rotation on incompatible schema {} > 0 && {}? {}",
        recordCount,
        incompatibleSchema,
        schemaRotation
    );
    return schemaRotation;
  }

  private boolean shouldRotateOnSize() {
    var messageSizeRotation = recordCount >= flushSize;
    log.trace("Rotation on size evaluation {} >= {}? {}",
        recordCount,
        flushSize,
        messageSizeRotation
    );
    return messageSizeRotation;
  }

  private boolean shouldRotateOnTime() {
    // TODO consider record timestamp to avoid high deli8very latency
    if (recordCount <= 0) {
      log.debug("No records has been processed, skipping rotation on time");
      return false;
    }
    var now = wallClock.millis();
    var shouldRotate = lastCommitTimestamp + wallClockRotateIntervalMs <= now;
    log.trace("Rotation on time evaluation {} + {} <= {}? {}",
        lastCommitTimestamp,
        wallClockRotateIntervalMs,
        now,
        shouldRotate);
    return shouldRotate;
  }

  private void commit() {
    if (icebergWriter != null && !icebergWriter.isComplete()) {
      log.debug("Committing {} records", recordCount);
      icebergWriter.complete();
    }
    recordCount = 0;
    committableOffsets.putAll(currentOffsets);
    lastCommitTimestamp = wallClock.millis();
    fileSchema = null;
  }

  private boolean report(SinkRecord sinkRecord, Exception e) {
    if (reporter == null) {
      return false;
    }
    reporter.report(sinkRecord, e);
    log.warn(
        "Errant record from topic {} partition {} offset {} written to DLQ due to: {}",
        sinkRecord.topic(),
        sinkRecord.kafkaPartition(),
        sinkRecord.kafkaOffset(),
        e.getMessage());
    return true;
  }
}
