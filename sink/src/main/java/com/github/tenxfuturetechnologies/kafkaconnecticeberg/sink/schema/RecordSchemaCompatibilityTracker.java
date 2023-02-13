package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.schema;

import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.IcebergSinkConnectorConfig;
import io.confluent.connect.storage.schema.StorageSchemaCompatibility;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

public class RecordSchemaCompatibilityTracker {
  private final StorageSchemaCompatibility compatibility;
  private Schema lastSeenSchema;

  public RecordSchemaCompatibilityTracker(IcebergSinkConnectorConfig config) {
    this(StorageSchemaCompatibility.getCompatibility(config.getSchemaCompatibility()));
  }

  // Visible for testing
  RecordSchemaCompatibilityTracker(StorageSchemaCompatibility compatibility) {
    this.compatibility = compatibility;
  }

  public void updateSchema(SinkRecord sinkRecord) {
    lastSeenSchema = sinkRecord.valueSchema();
  }

  public boolean isIncompatible(SinkRecord sinkRecord) {
    if (lastSeenSchema == null) {
      updateSchema(sinkRecord);
      return false;
    }
    return compatibility.shouldChangeSchema(sinkRecord, null, lastSeenSchema);
  }

  public SinkRecord project(SinkRecord sinkRecord) {
    if (lastSeenSchema == null) {
      throw new IllegalStateException("No records have been tracked, did you invoke updateSchema() "
          + "or isCompatible()?");
    }
    return compatibility.project(sinkRecord, null, lastSeenSchema);
  }
}
