package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.converter;

import static io.confluent.connect.avro.AvroData.GENERALIZED_TYPE_ENUM;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.IcebergSinkConnectorConfig;
import java.util.Arrays;
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
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SchemaConverterTest {

  private @Mock IcebergSinkConnectorConfig config;

  private SchemaConverter underTest;

  @BeforeEach
  void init() {
    underTest = new SchemaConverter(config);
  }

  @Test
  void shouldConvertKafkaConnectSchemaToIcebergSchema() {
    var testRecordSchema = SchemaBuilder.struct()
        .name("TestRecord")
        .optional()
        .field("id", Schema.INT32_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .build();
    var testEnumSchemaBuilder = SchemaBuilder.string()
        .name("TestEnum")
        .optional()
        .parameter(GENERALIZED_TYPE_ENUM, "TestEnum");
    Arrays.stream(TestEnum.values()).forEach(
        te -> testEnumSchemaBuilder.parameter(GENERALIZED_TYPE_ENUM + "." + te.name(), te.name()));
    var testEnumSchema = testEnumSchemaBuilder.build();
    var testArraySchema = SchemaBuilder.array(SchemaBuilder.string())
        .optional()
        .build();
    var testMapSchema = SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.int64())
        .optional()
        .build();
    var decimalSchema = Decimal.builder(2)
        .parameter("connect.decimal.precision", "6")
        .optional()
        .build();
    var kafkaConnectSchema = SchemaBuilder.struct()
        .name("TestEvent")
        .field("boolField", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("intField", Schema.OPTIONAL_INT32_SCHEMA)
        .field("longField", Schema.OPTIONAL_INT64_SCHEMA)
        .field("floatField", Schema.OPTIONAL_FLOAT32_SCHEMA)
        .field("doubleField", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("bytesField", Schema.OPTIONAL_BYTES_SCHEMA)
        .field("stringField", Schema.OPTIONAL_STRING_SCHEMA)
        .field("recordField", testRecordSchema)
        .field("enumField", testEnumSchema)
        .field("arrayField", testArraySchema)
        .field("mapField", testMapSchema)
        .field("decimalLogical", decimalSchema)
        .field("dateLogical", Date.builder().optional().build())
        .field("timeLogical", Time.builder().optional().build())
        .field("timestampLogical", Timestamp.builder().optional().build()).build();

    var icebergSchema = underTest.convert(kafkaConnectSchema);

    assertThat(icebergSchema)
        .isNotNull()
        .extracting(org.apache.iceberg.Schema::columns)
        .asList()
        .containsExactly(
            NestedField.of(0, true, "boolField", BooleanType.get()),
            NestedField.of(1, true, "intField", IntegerType.get()),
            NestedField.of(2, true, "longField", LongType.get()),
            NestedField.of(3, true, "floatField", FloatType.get()),
            NestedField.of(4, true, "doubleField", DoubleType.get()),
            NestedField.of(5, true, "bytesField", BinaryType.get()),
            NestedField.of(6, true, "stringField", StringType.get()),
            NestedField.of(7, true, "recordField",
                StructType.of(
                    NestedField.of(15, false, "id", IntegerType.get()),
                    NestedField.of(16, false, "name", StringType.get()))),
            NestedField.of(8, true, "enumField", StringType.get()),
            NestedField.of(9, true, "arrayField", ListType.ofRequired(17, StringType.get())),
            NestedField.of(10, true, "mapField",
                MapType.ofRequired(18, 19, StringType.get(), LongType.get())),
            NestedField.of(11, true, "decimalLogical", DecimalType.of(6, 2)),
            NestedField.of(12, true, "dateLogical", DateType.get()),
            NestedField.of(13, true, "timeLogical", TimeType.get()),
            NestedField.of(14, true, "timestampLogical", TimestampType.withoutZone()));
  }

  public enum TestEnum {
    A, B, C
  }
}