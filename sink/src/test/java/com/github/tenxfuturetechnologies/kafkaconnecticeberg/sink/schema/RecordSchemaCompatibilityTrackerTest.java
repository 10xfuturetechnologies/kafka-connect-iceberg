package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.confluent.connect.storage.schema.StorageSchemaCompatibility;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RecordSchemaCompatibilityTrackerTest {

  private @Mock StorageSchemaCompatibility storageSchemaCompatibility;
  private @Mock SinkRecord sinkRecord;
  private @Mock Schema schema;

  private RecordSchemaCompatibilityTracker underTest;

  @BeforeEach
  void init() {
    underTest = new RecordSchemaCompatibilityTracker(storageSchemaCompatibility);
  }

  @Test
  void shouldUpdateLastSeenSchemaWithSchemaFromRecord() {
    when(sinkRecord.valueSchema()).thenReturn(schema);

    underTest.updateSchema(sinkRecord);

    verifyNoMoreInteractions(sinkRecord);
    verifyNoInteractions(storageSchemaCompatibility);
  }

  @Test
  void shouldAlwaysBeCompatibleWhenNoRecordsHaveBeenPreviouslyTracked() {
    when(sinkRecord.valueSchema()).thenReturn(schema);

    var isCompatible = underTest.isIncompatible(sinkRecord);

    assertThat(isCompatible).isFalse();
    verifyNoMoreInteractions(sinkRecord);
    verifyNoInteractions(storageSchemaCompatibility);
  }

  @Test
  void shouldBeIncompatibleWhenStorageCompatibilityReturnsTrue() {
    var newRecord = mock(SinkRecord.class);
    when(sinkRecord.valueSchema()).thenReturn(schema);
    when(storageSchemaCompatibility.shouldChangeSchema(newRecord, null, schema))
            .thenReturn(true);

    underTest.isIncompatible(sinkRecord);
    var isCompatible = underTest.isIncompatible(newRecord);

    assertThat(isCompatible).isTrue();
    verifyNoMoreInteractions(storageSchemaCompatibility, sinkRecord, newRecord);
  }

  @Test
  void shouldBeCompatibleWhenStorageCompatibilityReturnsFalse() {
    var newRecord = mock(SinkRecord.class);
    when(sinkRecord.valueSchema()).thenReturn(schema);
    when(storageSchemaCompatibility.shouldChangeSchema(newRecord, null, schema))
            .thenReturn(false);

    underTest.isIncompatible(sinkRecord);
    var isCompatible = underTest.isIncompatible(newRecord);

    assertThat(isCompatible).isFalse();
    verifyNoMoreInteractions(storageSchemaCompatibility, sinkRecord, newRecord);
  }

  @Test
  void shouldPassThroughRecordProjection() {
    var newRecord = mock(SinkRecord.class);
    var projectedRecord = mock(SinkRecord.class);
    when(storageSchemaCompatibility.project(newRecord, null, schema))
            .thenReturn(projectedRecord);
    when(sinkRecord.valueSchema()).thenReturn(schema);

    underTest.updateSchema(sinkRecord);
    var pr = underTest.project(newRecord);

    assertThat(pr).isSameAs(projectedRecord);
    verifyNoMoreInteractions(storageSchemaCompatibility);
  }

  @Test
  void shouldThrowExceptionWhenCurrentSchemaIsNotInitialised() {
    var e = assertThrows(IllegalStateException.class, () -> underTest.project(sinkRecord));

    assertThat(e).hasMessage("No records have been tracked, did you invoke updateSchema() or"
            + " isCompatible()?");
    verifyNoInteractions(storageSchemaCompatibility, sinkRecord);
  }

  @Test
  void shouldThrowExceptionWhenStorageSchemaCompatibilityThrowsException() {
    var newRecord = mock(SinkRecord.class);
    var ex = new RuntimeException("error");
    when(storageSchemaCompatibility.project(newRecord, null, schema))
            .thenThrow(ex);
    when(sinkRecord.valueSchema()).thenReturn(schema);

    underTest.updateSchema(sinkRecord);
    var e = assertThrows(RuntimeException.class, () -> underTest.project(newRecord));

    assertThat(e).isSameAs(ex);
  }
}
