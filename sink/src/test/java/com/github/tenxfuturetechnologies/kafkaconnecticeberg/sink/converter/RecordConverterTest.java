package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.converter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.IcebergSinkConnectorConfig;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DateType;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimeType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RecordConverterTest {

  private static final String TOPIC = "topic";
  private static final int PARTITION = 0;
  private static final long OFFSET = 0L;
  private static final Schema KEY_SCHEMA = SchemaBuilder.string();
  private static final String KEY = "key";
  private static final String RECORD_TYPE = "EVENT";
  private static final ZoneId TIMEZONE = ZoneId.of("UTC");

  private @Mock IcebergSinkConnectorConfig config;

  private RecordConverter underTest;

  @BeforeEach
  void init() {
    when(config.getTimezone()).thenReturn(TIMEZONE);
    underTest = new RecordConverter(config);
  }

  @Test
  void shouldConvertBoolean() {
    var valueSchema = SchemaBuilder.struct()
        .name(RECORD_TYPE)
        .field("boolField", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build();
    var value = new Struct(valueSchema).put("boolField", true);
    var sinkRecord = new SinkRecord(
        TOPIC,
        PARTITION,
        KEY_SCHEMA,
        KEY,
        valueSchema,
        value,
        OFFSET);
    var tableSchema = new org.apache.iceberg.Schema(
        List.of(
            NestedField.of(0, true, "boolField", BooleanType.get())
        )
    );

    var icebergRecord = underTest.convert(sinkRecord, tableSchema);

    assertThat(icebergRecord.getField("boolField"))
        .isInstanceOf(Boolean.class)
        .isEqualTo(true);
  }

  @Test
  void shouldConvertInteger() {
    var valueSchema = SchemaBuilder.struct()
        .name(RECORD_TYPE)
        .field("intField", Schema.OPTIONAL_INT32_SCHEMA)
        .build();
    var value = new Struct(valueSchema).put("intField", 123);
    var sinkRecord = new SinkRecord(
        TOPIC,
        PARTITION,
        KEY_SCHEMA,
        KEY,
        valueSchema,
        value,
        OFFSET);
    var tableSchema = new org.apache.iceberg.Schema(
        List.of(
            NestedField.of(0, true, "intField", IntegerType.get())
        )
    );

    var icebergRecord = underTest.convert(sinkRecord, tableSchema);

    assertThat(icebergRecord.getField("intField"))
        .isInstanceOf(Integer.class)
        .isEqualTo(123);
  }

  @Test
  void shouldConvertLong() {
    var valueSchema = SchemaBuilder.struct()
        .name(RECORD_TYPE)
        .field("longField", Schema.OPTIONAL_INT64_SCHEMA)
        .build();
    var value = new Struct(valueSchema).put("longField", 123456L);
    var sinkRecord = new SinkRecord(
        TOPIC,
        PARTITION,
        KEY_SCHEMA,
        KEY,
        valueSchema,
        value,
        OFFSET);
    var tableSchema = new org.apache.iceberg.Schema(
        List.of(
            NestedField.of(0, true, "longField", LongType.get())
        )
    );

    var icebergRecord = underTest.convert(sinkRecord, tableSchema);

    assertThat(icebergRecord.getField("longField"))
        .isInstanceOf(Long.class)
        .isEqualTo(123456L);
  }

  @Test
  void shouldConvertFloat() {
    var valueSchema = SchemaBuilder.struct()
        .name(RECORD_TYPE)
        .field("floatField", Schema.OPTIONAL_FLOAT32_SCHEMA)
        .build();
    var value = new Struct(valueSchema).put("floatField", 1.2F);
    var sinkRecord = new SinkRecord(
        TOPIC,
        PARTITION,
        KEY_SCHEMA,
        KEY,
        valueSchema,
        value,
        OFFSET);
    var tableSchema = new org.apache.iceberg.Schema(
        List.of(
            NestedField.of(0, true, "floatField", FloatType.get())
        )
    );

    var icebergRecord = underTest.convert(sinkRecord, tableSchema);

    assertThat(icebergRecord.getField("floatField"))
        .isInstanceOf(Float.class)
        .isEqualTo(1.2F);
  }

  @Test
  void shouldConvertDouble() {
    var valueSchema = SchemaBuilder.struct()
        .name(RECORD_TYPE)
        .field("doubleField", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .build();
    var value = new Struct(valueSchema).put("doubleField", 3.14D);
    var sinkRecord = new SinkRecord(
        TOPIC,
        PARTITION,
        KEY_SCHEMA,
        KEY,
        valueSchema,
        value,
        OFFSET);
    var tableSchema = new org.apache.iceberg.Schema(
        List.of(
            NestedField.of(0, true, "doubleField", DoubleType.get())
        )
    );

    var icebergRecord = underTest.convert(sinkRecord, tableSchema);

    assertThat(icebergRecord.getField("doubleField"))
        .isInstanceOf(Double.class)
        .isEqualTo(3.14D);
  }

  @Test
  void shouldConvertDate() {
    var valueSchema = SchemaBuilder.struct()
        .name(RECORD_TYPE)
        .field("dateField", Date.SCHEMA)
        .build();
    var instant = Instant.EPOCH.plus(19350, ChronoUnit.DAYS);
    var value = new Struct(valueSchema).put("dateField", java.util.Date.from(instant));
    var sinkRecord = new SinkRecord(
        TOPIC,
        PARTITION,
        KEY_SCHEMA,
        KEY,
        valueSchema,
        value,
        OFFSET);
    var tableSchema = new org.apache.iceberg.Schema(
        List.of(
            NestedField.of(0, true, "dateField", DateType.get())
        )
    );

    var icebergRecord = underTest.convert(sinkRecord, tableSchema);

    assertThat(icebergRecord.getField("dateField"))
        .isInstanceOf(LocalDate.class)
        .isEqualTo(LocalDate.of(2022, 12, 24));
  }

  @Test
  void shouldConvertTime() {
    var valueSchema = SchemaBuilder.struct()
        .name(RECORD_TYPE)
        .field("timeField", Time.SCHEMA)
        .build();
    var instant = Instant.EPOCH
        .plus(10, ChronoUnit.HOURS)
        .plus(2, ChronoUnit.MINUTES)
        .plus(8, ChronoUnit.SECONDS);
    var value = new Struct(valueSchema).put("timeField", java.util.Date.from(instant));
    var sinkRecord = new SinkRecord(
        TOPIC,
        PARTITION,
        KEY_SCHEMA,
        KEY,
        valueSchema,
        value,
        OFFSET);
    var tableSchema = new org.apache.iceberg.Schema(
        List.of(
            NestedField.of(0, true, "timeField", TimeType.get())
        )
    );

    var icebergRecord = underTest.convert(sinkRecord, tableSchema);

    assertThat(icebergRecord.getField("timeField"))
        .isInstanceOf(LocalTime.class)
        .isEqualTo(LocalTime.of(10, 2, 8));
  }

  @Test
  void shouldConvertTimestamp() {
    var valueSchema = SchemaBuilder.struct()
        .name(RECORD_TYPE)
        .field("timestampField", Timestamp.SCHEMA)
        .build();
    var instant = Instant.now();
    var value = new Struct(valueSchema).put("timestampField", java.util.Date.from(instant));
    var sinkRecord = new SinkRecord(
        TOPIC,
        PARTITION,
        KEY_SCHEMA,
        KEY,
        valueSchema,
        value,
        OFFSET);
    var tableSchema = new org.apache.iceberg.Schema(
        List.of(
            NestedField.of(0, true, "timestampField", TimestampType.withoutZone())
        )
    );

    var icebergRecord = underTest.convert(sinkRecord, tableSchema);

    assertThat(icebergRecord.getField("timestampField"))
        .isInstanceOf(LocalDateTime.class)
        .isEqualTo(instant.atZone(TIMEZONE).truncatedTo(ChronoUnit.MILLIS).toLocalDateTime());
  }

  @Test
  void shouldConvertString() {
    var valueSchema = SchemaBuilder.struct()
        .name(RECORD_TYPE)
        .field("stringField", Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    var instant = Instant.now();
    var value = new Struct(valueSchema).put("stringField", "something");
    var sinkRecord = new SinkRecord(
        TOPIC,
        PARTITION,
        KEY_SCHEMA,
        KEY,
        valueSchema,
        value,
        OFFSET);
    var tableSchema = new org.apache.iceberg.Schema(
        List.of(
            NestedField.of(0, true, "stringField", StringType.get())
        )
    );

    var icebergRecord = underTest.convert(sinkRecord, tableSchema);

    assertThat(icebergRecord.getField("stringField"))
        .isInstanceOf(String.class)
        .isEqualTo("something");
  }

  @Test
  void shouldConvertBytesArray() {
    var valueSchema = SchemaBuilder.struct()
        .name(RECORD_TYPE)
        .field("bytesField", Schema.OPTIONAL_BYTES_SCHEMA)
        .build();
    var value = new Struct(valueSchema).put("bytesField", new byte[]{64});
    var sinkRecord = new SinkRecord(
        TOPIC,
        PARTITION,
        KEY_SCHEMA,
        KEY,
        valueSchema,
        value,
        OFFSET);
    var tableSchema = new org.apache.iceberg.Schema(
        List.of(
            NestedField.of(0, true, "bytesField", BinaryType.get())
        )
    );

    var icebergRecord = underTest.convert(sinkRecord, tableSchema);

    assertThat(icebergRecord.getField("bytesField"))
        .isInstanceOf(ByteBuffer.class)
        .isEqualTo(ByteBuffer.wrap(new byte[]{64}));
  }

  @Test
  void shouldConvertBytesBuffer() {
    var valueSchema = SchemaBuilder.struct()
        .name(RECORD_TYPE)
        .field("bytesField", Schema.OPTIONAL_BYTES_SCHEMA)
        .build();
    var value = new Struct(valueSchema).put("bytesField", ByteBuffer.wrap(new byte[]{64}));
    var sinkRecord = new SinkRecord(
        TOPIC,
        PARTITION,
        KEY_SCHEMA,
        KEY,
        valueSchema,
        value,
        OFFSET);
    var tableSchema = new org.apache.iceberg.Schema(
        List.of(
            NestedField.of(0, true, "bytesField", BinaryType.get())
        )
    );

    var icebergRecord = underTest.convert(sinkRecord, tableSchema);

    assertThat(icebergRecord.getField("bytesField"))
        .isInstanceOf(ByteBuffer.class)
        .isEqualTo(ByteBuffer.wrap(new byte[]{64}));
  }

  @Test
  void shouldConvertBigDecimalBytes() {
    var valueSchema = SchemaBuilder.struct()
        .name(RECORD_TYPE)
        .field("bigDecimalField", SchemaBuilder.bytes()
            .parameter("scale", "3")
            .build())
        .build();
    var value = new Struct(valueSchema)
        .put(
            "bigDecimalField",
            BigDecimal.valueOf(12345, 3).unscaledValue().toByteArray());
    var sinkRecord = new SinkRecord(
        TOPIC,
        PARTITION,
        KEY_SCHEMA,
        KEY,
        valueSchema,
        value,
        OFFSET);
    var tableSchema = new org.apache.iceberg.Schema(
        List.of(
            NestedField.of(0, true, "bigDecimalField", DecimalType.of(5, 2))
        )
    );

    var icebergRecord = underTest.convert(sinkRecord, tableSchema);

    assertThat(icebergRecord.getField("bigDecimalField"))
        .isInstanceOf(BigDecimal.class)
        .isEqualTo(BigDecimal.valueOf(12345, 3));
  }

  @Test
  void shouldConvertBigDecimal() {
    var valueSchema = SchemaBuilder.struct()
        .name(RECORD_TYPE)
        .field("bigDecimalField",
            Decimal.builder(3).parameter("connect.decimal.precision", "5").build())
        .build();
    var value = new Struct(valueSchema).put("bigDecimalField", BigDecimal.valueOf(12345, 3));
    var sinkRecord = new SinkRecord(
        TOPIC,
        PARTITION,
        KEY_SCHEMA,
        KEY,
        valueSchema,
        value,
        OFFSET);
    var tableSchema = new org.apache.iceberg.Schema(
        List.of(
            NestedField.of(0, true, "bigDecimalField", DecimalType.of(5, 2))
        )
    );

    var icebergRecord = underTest.convert(sinkRecord, tableSchema);

    assertThat(icebergRecord.getField("bigDecimalField"))
        .isInstanceOf(BigDecimal.class)
        .isEqualTo(BigDecimal.valueOf(12345, 3));
  }

  @Test
  void shouldConvertStruct() {
    var structSchema = SchemaBuilder.struct()
        .field("a", Schema.INT64_SCHEMA)
        .field("b", Schema.STRING_SCHEMA)
        .build();
    var valueSchema = SchemaBuilder.struct()
        .name(RECORD_TYPE)
        .field("structField", structSchema)
        .build();
    var value = new Struct(valueSchema).put("structField",
        new Struct(structSchema).put("a", 123L).put("b", "test"));
    var sinkRecord = new SinkRecord(
        TOPIC,
        PARTITION,
        KEY_SCHEMA,
        KEY,
        valueSchema,
        value,
        OFFSET);
    var structFieldSchema = StructType.of(
        List.of(
            NestedField.of(1, false, "a", LongType.get()),
            NestedField.of(2, false, "b", StringType.get())));
    var tableSchema = new org.apache.iceberg.Schema(
        List.of(
            NestedField.of(0, true, "structField", structFieldSchema)
        )
    );

    var icebergRecord = underTest.convert(sinkRecord, tableSchema);

    var result = GenericRecord.create(structFieldSchema);
    result.setField("a", 123L);
    result.setField("b", "test");
    assertThat(icebergRecord.getField("structField"))
        .isInstanceOf(GenericRecord.class)
        .isEqualTo(result);
  }

  @Test
  void shouldConvertList() {
    var valueSchema = SchemaBuilder.struct()
        .name(RECORD_TYPE)
        .field("arrayField", SchemaBuilder.array(Schema.STRING_SCHEMA))
        .build();
    var value = new Struct(valueSchema).put("arrayField", List.of("a", "b", "c"));
    var sinkRecord = new SinkRecord(
        TOPIC,
        PARTITION,
        KEY_SCHEMA,
        KEY,
        valueSchema,
        value,
        OFFSET);
    var tableSchema = new org.apache.iceberg.Schema(
        List.of(
            NestedField.of(0, true, "arrayField", ListType.ofRequired(1, StringType.get()))
        )
    );

    var icebergRecord = underTest.convert(sinkRecord, tableSchema);

    assertThat(icebergRecord.getField("arrayField"))
        .isInstanceOf(List.class)
        .isEqualTo(List.of("a", "b", "c"));
  }

  @Test
  void shouldConvertMap() {
    var valueSchema = SchemaBuilder.struct()
        .name(RECORD_TYPE)
        .field("mapField", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA))
        .build();
    var value = new Struct(valueSchema).put("mapField", Map.of("a", true, "b", false));
    var sinkRecord = new SinkRecord(
        TOPIC,
        PARTITION,
        KEY_SCHEMA,
        KEY,
        valueSchema,
        value,
        OFFSET);
    var tableSchema = new org.apache.iceberg.Schema(
        List.of(
            NestedField.of(0, true, "mapField",
                MapType.ofRequired(1, 2, StringType.get(), BooleanType.get()))
        )
    );

    var icebergRecord = underTest.convert(sinkRecord, tableSchema);

    assertThat(icebergRecord.getField("mapField"))
        .isInstanceOf(Map.class)
        .isEqualTo(Map.of("a", true, "b", false));
  }


  @Test
  void shouldConvertNestedType() {
    var nestedType1Schema = SchemaBuilder.struct()
        .field("x", Schema.STRING_SCHEMA)
        .field("y", Schema.FLOAT32_SCHEMA)
        .build();
    var nestedType2Schema = SchemaBuilder.map(Schema.STRING_SCHEMA, nestedType1Schema);
    var structSchema = SchemaBuilder.struct()
        .field("a", SchemaBuilder.array(nestedType1Schema))
        .field("b", nestedType2Schema)
        .build();
    var valueSchema = SchemaBuilder.struct()
        .name(RECORD_TYPE)
        .field("structField", structSchema)
        .build();
    var value = new Struct(valueSchema)
        .put(
            "structField",
            new Struct(structSchema)
                .put("a", List.of(
                    new Struct(nestedType1Schema).put("x", "val1").put("y", 1.3F),
                    new Struct(nestedType1Schema).put("x", "val2").put("y", 6.7F)))
                .put("b", Map.of(
                    "k1", new Struct(nestedType1Schema).put("x", "k1.1").put("y", 0.5F),
                    "k2", new Struct(nestedType1Schema).put("x", "k2.1").put("y", 0.9F))));
    var sinkRecord = new SinkRecord(
        TOPIC,
        PARTITION,
        KEY_SCHEMA,
        KEY,
        valueSchema,
        value,
        OFFSET);
    var tableSchema = new org.apache.iceberg.Schema(
        List.of(
            NestedField.of(
                0,
                true,
                "structField",
                StructType.of(
                    NestedField.of(
                        1,
                        false,
                        "a",
                        ListType.ofOptional(3, StructType.of(
                            List.of(
                                NestedField.of(6, false, "x", StringType.get()),
                                NestedField.of(7, false, "y", FloatType.get())))
                        )),
                    NestedField.of(
                        2,
                        false,
                        "b",
                        MapType.ofRequired(4, 5, StringType.get(), StructType.of(
                            List.of(
                                NestedField.of(8, false, "x", StringType.get()),
                                NestedField.of(9, false, "y", FloatType.get())))
                        )))
            ))
    );

    final var icebergRecord = underTest.convert(sinkRecord, tableSchema);

    var listElementType = tableSchema.findField("structField")
        .type()
        .asStructType()
        .field("a")
        .type()
        .asListType()
        .elementType()
        .asStructType();
    var listRecord0 = GenericRecord.create(listElementType);
    listRecord0.setField("x", "val1");
    listRecord0.setField("y", 1.3F);
    var listRecord1 = GenericRecord.create(listElementType);
    listRecord1.setField("x", "val2");
    listRecord1.setField("y", 6.7F);
    var mapValueType = tableSchema.findField("structField")
        .type()
        .asStructType()
        .field("b")
        .type()
        .asMapType()
        .valueType()
        .asStructType();
    var mapRecordK1 = GenericRecord.create(mapValueType);
    mapRecordK1.setField("x", "k1.1");
    mapRecordK1.setField("y", 0.5F);
    var mapRecordK2 = GenericRecord.create(mapValueType);
    mapRecordK2.setField("x", "k2.1");
    mapRecordK2.setField("y", 0.9F);
    var structField = GenericRecord.create(
        tableSchema.findField("structField")
            .type()
            .asStructType());
    structField.setField("a", List.of(listRecord0, listRecord1));
    structField.setField("b", Map.of("k1", mapRecordK1, "k2", mapRecordK2));
    var result = GenericRecord.create(tableSchema);
    result.setField("structField", structField);
    assertThat(icebergRecord).isEqualTo(result);
  }

  public enum TestEnum {
    A, B, C
  }
}