package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.iceberg;

import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.spi.IcebergWriter;
import java.io.IOException;
import java.util.Arrays;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultIcebergWriter implements IcebergWriter {

  private static final Logger log = LoggerFactory.getLogger(DefaultIcebergWriter.class);

  private final Table table;
  private final BaseTaskWriter<Record> writer;
  private long counter;
  private boolean complete;

  public DefaultIcebergWriter(Table table, BaseTaskWriter<Record> writer) {
    this.table = table;
    this.writer = writer;
    counter = 0;
    complete = false;
  }

  @Override
  public void write(Record record) {
    guard();
    try {
      writer.write(record);
      ++counter;
    } catch (Exception e) {
      throw new ConnectException("Unable to write Iceberg record", e);
    }
  }

  @Override
  public void complete() {
    guard();
    try {
      close();
      log.debug("Completing Iceberg writer for table {}", table.name());
      var result = writer.complete();
      if (counter <= 0) {
        log.debug("No records has been written for table {}", table.name());
        complete = true;
        return;
      }

      IcebergUtils.transactional(() -> {
        table.refresh();
        var transaction = table.newTransaction();
        var rowDelta = transaction.newRowDelta();
        Arrays.stream(result.dataFiles())
            .filter(f -> f.recordCount() > 0)
            .forEach(f -> {
              log.debug("Adding file to table {} partition {}", table.name(), f.partition());
              rowDelta.addRows(f);
            });
        Arrays.stream(result.deleteFiles())
            .filter(f -> f.recordCount() > 0)
            .forEach(f -> {
              log.debug("Adding delete file to table {} partition {}", table.name(), f.partition());
              rowDelta.addDeletes(f);
            });
        rowDelta.commit();
        transaction.commitTransaction();
        complete = true;
        log.debug("Committed {} events to table {}", counter, table.location());
      });
    } catch (Exception e) {
      throw new ConnectException("Unable to commit delta files", e);
    }
  }

  @Override
  public void close() {
    guard();
    try {
      log.debug("Closing writer for table {}", table.name());
      writer.close();
    } catch (Exception e) {
      throw new ConnectException("Unable to close delta files", e);
    }
  }

  @Override
  public boolean isComplete() {
    return complete;
  }

  private void guard() {
    if (complete) {
      throw new IllegalStateException(
              "This writer is now complete, a new instance must be created");
    }
  }
}
