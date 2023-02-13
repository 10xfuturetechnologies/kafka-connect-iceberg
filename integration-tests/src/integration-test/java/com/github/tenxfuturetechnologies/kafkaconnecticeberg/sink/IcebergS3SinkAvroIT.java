package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink;

import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.TABLE_NAME;
import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.TABLE_NAMESPACE;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.given;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.avro.test.TestEnum;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.avro.test.TestEvent;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.avro.test.TestRecord;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.SinkConfigBuilder;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.assertj.core.util.Files;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

public class IcebergS3SinkAvroIT extends AbstractBaseIT {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(60);
  private static final String CONNECTOR_NAME = "iceberg-sink";
  private static final Configuration HADOOP_CONF = new Configuration();


  private KafkaProducer<String, GenericRecord> kafkaProducer;

  private static String serialiseSinkConfig(IcebergSinkConnectorConfig config, String topic) {
    var icebergConfig = MAPPER.createObjectNode();
    icebergConfig.put("name", CONNECTOR_NAME);
    var configNode = MAPPER.createObjectNode();
    config.originalsStrings().forEach(configNode::put);
    configNode.put("connector.class", IcebergSinkConnector.class.getName());
    configNode.put("topics", topic);
    configNode.put(
        "format.class",
        "io.confluent.connect.s3.format.avro.AvroFormat");
    configNode.put(
        "header.converter",
        "org.apache.kafka.connect.converters.ByteArrayConverter");
    configNode.put(
        "key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
    configNode.put(
        "key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
    configNode.put("key.converter.schemas.enable", "false");
    configNode.put("value.converter", "io.confluent.connect.avro.AvroConverter");
    configNode.put("value.converter.schemas.enable", "true");
    configNode.put(
        "value.converter.schema.registry.url",
        SCHEMA_REGISTRY_CONTAINER.getInternalUrl());
    icebergConfig.set("config", configNode);
    return icebergConfig.toString();
  }

  private static SinkConfigBuilder.Builder standardSinkConfigBuilder() {
    return SinkConfigBuilder.builder().withS3(MINIO_CONTAINER.getInternalUrl());
  }

  private static String standardSinkConfig(String topic) {
    return serialiseSinkConfig(standardSinkConfigBuilder().build(), topic);
  }

  private static String sinkConfigWithSchemaEvolution(String topic) {
    return serialiseSinkConfig(
        standardSinkConfigBuilder()
            .withTableSchemaEvolve(true)
            .build(),
        topic);
  }

  private static String sinkConfigWithRecordSchemaWriter(String topic) {
    return serialiseSinkConfig(
        standardSinkConfigBuilder()
            .withWriterKeepRecordFields(true)
            .build(),
        topic);
  }

  private static String sinkConfigWithWallClockRotateIntervalMs(String topic) {
    return serialiseSinkConfig(
        standardSinkConfigBuilder()
            .withFlushSettings(100, TimeUnit.SECONDS.toMillis(3))
            .build(),
        topic);
  }

  @BeforeEach
  void init() {
    kafkaProducer = createProducer(StringSerializer.class, KafkaAvroSerializer.class);
  }

  @AfterEach
  void destroy() {
    if (kafkaProducer != null) {
      kafkaProducer.close();
    }
    deleteConnector(CONNECTOR_NAME);
    getSparkTestHelper().dropTable(TABLE_NAME);
    getMinioTestHelper().deleteObjects(
        format("iceberg_warehouse/%s/%s", TABLE_NAMESPACE, TABLE_NAME));
  }

  @Test
  void shouldSinkEvents() throws Exception {
    final var topicName = "test-topic";
    createConnector(standardSinkConfig(topicName));
    waitTillConnectorIsRunning(CONNECTOR_NAME, WAIT_TIMEOUT);

    final var uuid = UUID.fromString("87386c4a-d284-491a-a501-b14c4efde56e");
    final var now = Instant.now();
    final var nowNanos = now.plusNanos(123456);
    var eventBuilder = TestEvent.newBuilder()
        .setBoolField(true)
        .setIntField(123)
        .setLongField(123456L)
        .setFloatField(1.5F)
        .setDoubleField(3.14D)
        .setBytesField(ByteBuffer.allocate(1).put(((byte) 64)))
        .setStringField("string val")
        .setRecordField(TestRecord.newBuilder().setId(9).setName("rec").build())
        .setEnumField(TestEnum.A)
        .setArrayField(List.of("x", "y", "z"))
        .setMapField(Map.of("a", 7L))
        .setDecimalLogical(BigDecimal.valueOf(578, 2))
        .setUuidLogical(uuid)
        .setDateLogical(LocalDate.of(2022, 1, 11))
        // Not supported by spark
        //.setTimeMillisLogical(LocalTime.of(12, 7, 58))
        //.setTimeMicrosLogical(LocalTime.of(12, 7, 59, 123456))
        .setTimestampMillisLogical(now)
        .setTimestampMicrosLogical(nowNanos)
        .setLocalTimestampMillisLogical(LocalDateTime.of(2022, 1, 11, 12, 16, 25))
        .setLocalTimestampMicrosLogical(LocalDateTime.of(2022, 1, 11, 12, 16, 41, 123456));
    Function<GenericRecord, ?> keyExtractor = e -> e.get("stringField");
    var event1 = eventBuilder
        .setStringField("string val 1")
        .build();
    sendRecord(event1, keyExtractor, topicName);
    var event2 = eventBuilder
        .setStringField("string val 2")
        .build();
    sendRecord(event2, keyExtractor, topicName);

    given().ignoreExceptions()
        .await()
        .atMost(WAIT_TIMEOUT)
        .until(() -> getSparkTestHelper().getTableData(TABLE_NAME).count() >= 2);

    var result = getSparkTestHelper().getTableData(TABLE_NAME);
    assertThat(result.count()).isEqualTo(2);
    var rows = toMapById(result.getRows(2, 0), r -> r.get(6));
    var currentDate = DateTimeFormatter.ofPattern("yyyyMMdd")
        .withZone(ZoneId.systemDefault())
        .format(now);
    var timestampMillisFormatter =
        new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.MILLI_OF_SECOND, 1, 3, true)
            .toFormatter()
            .withZone(ZoneId.systemDefault());
    for (var key : List.of("string val 1", "string val 2")) {
      var row = rows.get(key);
      var col = 0;
      assertThat(row.get(col++)).isEqualTo("true");
      assertThat(row.get(col++)).isEqualTo("123");
      assertThat(row.get(col++)).isEqualTo("123456");
      assertThat(row.get(col++)).isEqualTo("1.5");
      assertThat(row.get(col++)).isEqualTo("3.14");
      assertThat(row.get(col++)).isEqualTo("[]");
      assertThat(row.get(col++)).isEqualTo(key);
      assertThat(row.get(col++)).isEqualTo("{9, rec}");
      assertThat(row.get(col++)).isEqualTo("A");
      assertThat(row.get(col++)).isEqualTo("[x, y, z]");
      assertThat(row.get(col++)).isEqualTo("{a -> 7}");
      assertThat(row.get(col++)).isEqualTo("5.78");
      assertThat(row.get(col++)).isEqualTo("87386c4a-d284-491a-a501-b14c4efde56e");
      assertThat(row.get(col++)).isEqualTo("2022-01-11");
      assertThat(row.get(col++)).isEqualTo(timestampMillisFormatter.format(now));
      assertThat(row.get(col++)).isEqualTo(Long.toString(
          nowNanos.getEpochSecond() * 1000000 + nowNanos.get(ChronoField.MICRO_OF_SECOND)));
      assertThat(row.get(col++)).isEqualTo("1641903385000");
      assertThat(row.get(col++)).isEqualTo("1641903401000123");
      assertThat(row.get(col++))
          .startsWith(
              "s3a://test-bucket/iceberg_warehouse/iceberg_ns/test_table/data/" + currentDate + "-")
          .endsWith(".parquet");
    }
  }

  @Test
  void shouldEvolveTableSchema() throws Exception {
    final var topicName = "test-schema-evolution";
    createConnector(sinkConfigWithSchemaEvolution(topicName));
    waitTillConnectorIsRunning(CONNECTOR_NAME, WAIT_TIMEOUT);

    final var recordTypeName = "TestRecord";
    final var recordNamespace = "com.example.sink";
    final var schemaV1 = SchemaBuilder.record(recordTypeName)
        .namespace(recordNamespace)
        .fields()
        .requiredString("s")
        .endRecord();
    var record1 = new GenericData.Record(schemaV1);
    record1.put("s", "k1");
    sendRecord(record1, r -> r.get("s"), topicName);

    given().ignoreExceptions()
        .await()
        .atMost(WAIT_TIMEOUT)
        .until(() -> getSparkTestHelper().getTableData(TABLE_NAME).count() >= 1);

    var result = getSparkTestHelper().query(
        "SELECT * FROM " + getSparkTestHelper().fullyQualifiedTableName(TABLE_NAME));
    var rows = toMapById(result.getRows(1, 0), r -> r.get(0));
    assertThat(rows.get("k1")).hasSize(1);

    // Evolve table
    final var schemaV2 = SchemaBuilder.record(recordTypeName)
        .namespace(recordNamespace)
        .fields()
        .requiredString("s")
        .optionalInt("i")
        .endRecord();
    var record2 = new GenericData.Record(schemaV2);
    record2.put("s", "k2");
    record2.put("i", 123);
    sendRecord(record2, r -> r.get("s"), topicName);
    // New record with original schema
    var record3 = new GenericData.Record(schemaV1);
    record3.put("s", "k3");
    sendRecord(record3, r -> r.get("s"), topicName);

    given().ignoreExceptions()
        .await()
        .atMost(WAIT_TIMEOUT)
        .until(() -> getSparkTestHelper().getTableData(TABLE_NAME).count() >= 3);

    result = getSparkTestHelper().query(
        "SELECT * FROM " + getSparkTestHelper().fullyQualifiedTableName(TABLE_NAME));
    var moreRows = toMapById(result.getRows(3, 0), r -> r.get(0));
    assertThat(moreRows.get("k1")).hasSize(2).containsExactly("k1", "null");
    assertThat(moreRows.get("k2")).hasSize(2).contains("k2", "123");
    assertThat(moreRows.get("k3")).hasSize(2).containsExactly("k3", "null");
  }

  @Test
  void shouldFlushEventsAfterWallClockTimeout() throws Exception {
    final var topicName = "test-rotate-on-wall-clock";
    createConnector(sinkConfigWithWallClockRotateIntervalMs(topicName));
    waitTillConnectorIsRunning(CONNECTOR_NAME, WAIT_TIMEOUT);

    var event = TestEvent.newBuilder()
        .setStringField("string val 1")
        .build();
    sendRecord(event, e -> e.get("stringField"), topicName);

    Thread.sleep(TimeUnit.SECONDS.toMillis(3));
    given().ignoreExceptions()
        .await()
        .atMost(WAIT_TIMEOUT)
        .until(() -> getSparkTestHelper().getTableData(TABLE_NAME).count() > 0);

    var sql = "SELECT * FROM " + getSparkTestHelper().fullyQualifiedTableName(TABLE_NAME);
    var result = getSparkTestHelper().query(sql);
    assertThat(result.count()).isEqualTo(1);
    var rows = toMapById(result.getRows(1, 0), r -> r.get(6));
    var row = rows.get("string val 1");
    for (var col = 0; col < 18; ++col) {
      if (col == 6) {
        assertThat(row.get(col++)).isEqualTo("string val 1");
      } else {
        assertThat(row.get(col)).isEqualTo("null");
      }
    }
  }

  @Test
  void shouldPreserveRecordData() throws Exception {
    final var topicName = "test-data-files";
    createConnector(sinkConfigWithRecordSchemaWriter(topicName));
    waitTillConnectorIsRunning(CONNECTOR_NAME, WAIT_TIMEOUT);

    final var recordTypeName = "TestRecord";
    final var recordNamespace = "com.example.sink";

    final var schemaV1 = SchemaBuilder.record(recordTypeName)
        .namespace(recordNamespace)
        .fields()
        .requiredString("s")
        .endRecord();
    var record1 = new GenericData.Record(schemaV1);
    record1.put("s", "k1");
    sendRecord(record1, r -> r.get("s"), topicName);

    // Evolve record
    final var schemaV2 = SchemaBuilder.record(recordTypeName)
        .namespace(recordNamespace)
        .fields()
        .requiredString("s")
        .optionalInt("i")
        .endRecord();
    var record2 = new GenericData.Record(schemaV2);
    record2.put("s", "k2");
    record2.put("i", 123);
    sendRecord(record2, r -> r.get("s"), topicName);

    // Evolve record again
    final var schemaV3 = SchemaBuilder.record(recordTypeName)
        .namespace(recordNamespace)
        .fields()
        .requiredString("s")
        .optionalBoolean("b")
        .endRecord();
    var record3 = new GenericData.Record(schemaV3);
    record3.put("s", "k3");
    record3.put("b", true);
    sendRecord(record3, r -> r.get("s"), topicName);

    given().ignoreExceptions()
        .await()
        .atMost(WAIT_TIMEOUT)
        .until(() -> getSparkTestHelper().getTableData(TABLE_NAME).count() >= 3);

    var result = getSparkTestHelper().getTableData(TABLE_NAME);
    var rows = toMapById(result.getRows(3, 0), r -> r.get(0));
    var row1 = rows.get("k1");
    var row2 = rows.get("k2");
    var row3 = rows.get("k3");
    assertThat(row1.get(1))
        .isNotEqualTo(row2.get(1))
        .isNotEqualTo(row3.get(1));
    assertThat(row1).hasSize(2).contains("k1");
    var records1 = readParquetFile(row1.get(1));
    assertThat(records1).hasSize(1)
        .element(0)
        .satisfies(r -> {
          assertThat(r.hasField("s")).isTrue();
          assertThat(r.get("s").toString()).isEqualTo("k1");
        });
    assertThat(row2).hasSize(2).contains("k2");
    var records2 = readParquetFile(row2.get(1));
    assertThat(records2).hasSize(1)
        .element(0)
        .satisfies(r -> {
          assertThat(r.hasField("s")).isTrue();
          assertThat(r.get("s").toString()).isEqualTo("k2");
          assertThat(r.hasField("i")).isTrue();
          assertThat(r.get("i")).isEqualTo(123);
        });
    assertThat(row3).hasSize(2).contains("k3");
    var records3 = readParquetFile(row3.get(1));
    assertThat(records3).hasSize(1)
        .element(0)
        .satisfies(r -> {
          assertThat(r.hasField("s")).isTrue();
          assertThat(r.get("s").toString()).isEqualTo("k3");
          assertThat(r.hasField("b")).isTrue();
          assertThat(r.get("b")).isEqualTo(true);
        });
  }

  private List<GenericRecord> readParquetFile(String s3Key) {
    try {
      var f = downloadS3File(s3Key);
      var inputFile = HadoopInputFile.fromPath(new Path(f.toURI()), HADOOP_CONF);
      var reader = AvroParquetReader.<GenericRecord>builder(inputFile).build();
      var result = new ArrayList<GenericRecord>();
      for (var record = reader.read(); record != null; record = reader.read()) {
        result.add(record);
      }
      reader.close();
      f.deleteOnExit();
      return result;
    } catch (IOException e) {
      throw new RuntimeException("Unable to read location " + s3Key, e);
    }
  }

  private File downloadS3File(String s3Key) {
    try {
      var f = Files.newTemporaryFile();
      var fout = new FileOutputStream(f);
      getMinioTestHelper().getObject(s3Key, fout);
      fout.flush();
      fout.close();
      return f;
    } catch (IOException e) {
      throw new RuntimeException("Unable to download object " + s3Key, e);
    }
  }

  private void sendRecord(
      GenericRecord record,
      Function<GenericRecord, ?> partitionExtractor,
      String topic) throws Exception {
    var r = createProducerRecord(record, partitionExtractor, topic);
    kafkaProducer.send(r).get();
    kafkaProducer.flush();
  }

  private Map<String, List<String>> toMapById(
      Seq<Seq<String>> rows,
      Function<List<String>, String> keyExtractor) {
    return CollectionConverters.asJava(rows)
        .stream()
        .map(CollectionConverters::asJava)
        .collect(Collectors.toMap(
            keyExtractor,
            Function.identity(),
            (a, b) -> b,
            LinkedHashMap::new));
  }
}
