package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.converter;

import static org.apache.commons.lang3.time.DateUtils.MILLIS_PER_DAY;

import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.IcebergSinkConnectorConfig;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordConverter {

  private static final Logger log = LoggerFactory.getLogger(RecordConverter.class);

  private final ZoneId timezone;

  public RecordConverter(IcebergSinkConnectorConfig config) {
    timezone = config.getTimezone();
  }

  public GenericRecord convert(SinkRecord sinkRecord, org.apache.iceberg.Schema icebergSchema) {
    if (sinkRecord.valueSchema() == null) {
      log.debug("Got sink record with no schema");
      throw new DataException(
          "Cannot transform sink record to Iceberg record not knowing the schema");
    }
    return toIcebergRecord(icebergSchema.asStruct(), sinkRecord.value());
  }

  private org.apache.iceberg.data.GenericRecord toIcebergRecord(
      org.apache.iceberg.types.Types.StructType recordFields,
      Object sinkValue) {
    if (sinkValue == null) {
      log.debug("Got sink record with null value, returning null");
      return null;
    }
    if (sinkValue instanceof Struct) {
      log.debug("Converting Struct to Iceberg");
      return toIcebergRecord(recordFields, (Struct) sinkValue);
    } else if (sinkValue instanceof Map) {
      log.debug("Converting Map to Iceberg");
      // should not happen because we validate we have a schema
      // this could be implemented in the future
      throw new DataException("Cannot transform sink record to Iceberg because value is a Map");
    } else {
      log.debug("Cannot convert value to Iceberg, class is {}", sinkValue.getClass().getName());
      throw new DataException(
          "Cannot transform sink record to Iceberg record because class is "
              + sinkValue.getClass().getName());
    }
  }

  private org.apache.iceberg.data.GenericRecord toIcebergRecord(
      org.apache.iceberg.types.Types.StructType recordFields,
      Struct sinkValue) {
    var icebergRecord = org.apache.iceberg.data.GenericRecord.create(recordFields);
    recordFields
        .fields()
        .stream()
        .forEach(f -> icebergRecord.setField(f.name(), extractIcebergValue(f, sinkValue)));
    return icebergRecord;
  }

  private Object extractIcebergValue(
      org.apache.iceberg.types.Types.NestedField field,
      Struct sinkValue) {
    if (sinkValue == null) {
      return null;
    }
    var fieldValue = getSinkFieldValueOrNull(sinkValue, field.name());
    if (fieldValue == null) {
      return null;
    }
    var fieldSchema = getSinkSchema(sinkValue, field.name());
    return castValue(field.name(), field.type(), fieldValue, fieldSchema);
  }

  private Object castValue(
      String fieldName,
      org.apache.iceberg.types.Type fieldType,
      Object sinkValue,
      Schema sinkSchema) {
    if (sinkValue == null) {
      return null;
    }
    Object fieldValue = null;
    log.debug("Field {} is type {}", fieldName, fieldType.typeId());
    switch (fieldType.typeId()) {
      case BOOLEAN:
        fieldValue = toBoolean(fieldType, fieldName, sinkValue);
        break;
      case INTEGER:
        fieldValue = toInteger(fieldType, fieldName, sinkValue);
        break;
      case LONG:
        fieldValue = toLong(fieldType, fieldName, sinkValue);
        break;
      case FLOAT:
        fieldValue = toFloat(fieldType, fieldName, sinkValue);
        break;
      case DOUBLE:
        fieldValue = toDouble(fieldType, fieldName, sinkValue);
        break;
      case DATE:
        fieldValue = toDate(fieldType, fieldName, sinkValue, sinkSchema);
        break;
      case TIME:
        fieldValue = toTime(fieldType, fieldName, sinkValue, sinkSchema);
        break;
      case TIMESTAMP:
        fieldValue = toTimestamp(fieldType, fieldName, sinkValue, sinkSchema);
        break;
      case STRING:
        fieldValue = toString(fieldType, fieldName, sinkValue);
        break;
      case UUID:
        fieldValue = toUuid(fieldType, fieldName, sinkValue);
        break;
      case FIXED:
      case BINARY:
        fieldValue = toByteBuffer(fieldType, fieldName, sinkValue);
        break;
      case DECIMAL:
        fieldValue = toBigDecimal(fieldType, fieldName, sinkValue, sinkSchema);
        break;
      case STRUCT:
        fieldValue = toStruct(fieldType, fieldName, sinkValue);
        break;
      case LIST:
        fieldValue = toList(fieldType, fieldName, sinkValue, sinkSchema);
        break;
      case MAP:
        fieldValue = toMap(fieldType, fieldName, sinkValue, sinkSchema);
        break;
      default:
        throw new DataException("Unknown field type " + fieldType.typeId().name());
    }
    return fieldValue;
  }

  private Object getSinkFieldValueOrNull(Struct sinkValue, String fieldName) {
    try {
      return sinkValue.get(fieldName);
    } catch (DataException e) {
      return null;
    }
  }

  private Schema getSinkSchema(Struct sinkValue, String fieldName) {
    return sinkValue.schema().field(fieldName).schema();
  }

  private Boolean toBoolean(org.apache.iceberg.types.Type type, String name, Object value) {
    try {
      return (Boolean) value;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to convert value to " + type.typeId().name() + ", field: " + name,
          e);
    }
  }

  private Integer toInteger(org.apache.iceberg.types.Type type, String name, Object value) {
    try {
      return (Integer) value;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to convert value to " + type.typeId().name() + ", field: " + name,
          e);
    }
  }

  private Long toLong(org.apache.iceberg.types.Type type, String name, Object value) {
    try {
      return (Long) value;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to convert value to " + type.typeId().name() + ", field: " + name,
          e);
    }
  }

  private Float toFloat(org.apache.iceberg.types.Type type, String name, Object value) {
    try {
      return (Float) value;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to convert value to " + type.typeId().name() + ", field: " + name,
          e);
    }
  }

  private Double toDouble(org.apache.iceberg.types.Type type, String name, Object value) {
    try {
      return (Double) value;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to convert value to " + type.typeId().name() + ", field: " + name,
          e);
    }
  }

  private LocalDate toDate(
      org.apache.iceberg.types.Type type,
      String name,
      Object value,
      Schema schema) {
    try {
      if (value instanceof LocalDate) {
        return (LocalDate) value;
      }
      if (value instanceof java.util.Date) {
        value = Date.fromLogical(schema, (java.util.Date) value);
      }
      var longVal = value instanceof Long ? (Long) value : ((Integer) value).longValue();
      // TODO allow providing a converter via config
      var instant = Instant.ofEpochMilli(longVal * MILLIS_PER_DAY);
      return LocalDate.ofInstant(instant, timezone);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to convert value to " + type.typeId().name() + ", field: " + name,
          e);
    }
  }

  private LocalTime toTime(
      org.apache.iceberg.types.Type type,
      String name,
      Object value,
      Schema schema) {
    try {
      if (value instanceof LocalTime) {
        return (LocalTime) value;
      }
      if (value instanceof java.util.Date) {
        value = Time.fromLogical(schema, (java.util.Date) value);
      }
      var longVal = value instanceof Long ? (Long) value : ((Integer) value).longValue();
      var instant = Instant.ofEpochMilli(longVal);
      return LocalTime.ofInstant(instant, timezone);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to convert value to " + type.typeId().name() + ", field: " + name,
          e);
    }
  }

  private LocalDateTime toTimestamp(
      org.apache.iceberg.types.Type type,
      String name,
      Object value,
      Schema schema) {
    try {
      if (value instanceof LocalDateTime) {
        return (LocalDateTime) value;
      }
      if (value instanceof java.util.Date) {
        value = Timestamp.fromLogical(schema, (java.util.Date) value);
      }
      var instant = Instant.ofEpochMilli((Long) value);
      return LocalDateTime.ofInstant(instant, timezone);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to convert value to " + type.typeId().name() + ", field: " + name,
          e);
    }
  }

  private String toString(org.apache.iceberg.types.Type type, String name, Object value) {
    try {
      return (String) value;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to convert value to " + type.typeId().name() + ", field: " + name,
          e);
    }
  }

  private UUID toUuid(org.apache.iceberg.types.Type type, String name, Object value) {
    try {
      if (value instanceof String) {
        return UUID.fromString(value.toString());
      }
      return (UUID) value;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to convert value to " + type.typeId().name() + ", field: " + name,
          e);
    }
  }

  private ByteBuffer toByteBuffer(org.apache.iceberg.types.Type type, String name, Object value) {
    try {
      if (value instanceof byte[]) {
        return ByteBuffer.wrap((byte[]) value);
      }
      return (ByteBuffer) value;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to convert value to " + type.typeId().name() + ", field: " + name,
          e);
    }
  }

  private BigDecimal toBigDecimal(
      org.apache.iceberg.types.Type type,
      String name,
      Object value,
      Schema schema) {
    try {
      if (value instanceof byte[]) {
        return Decimal.toLogical(schema, (byte[]) value);
      }
      return (BigDecimal) value;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to convert value to " + type.typeId().name() + ", field: " + name,
          e);
    }
  }

  private StructLike toStruct(org.apache.iceberg.types.Type type, String name, Object value) {
    try {
      return toIcebergRecord(type.asStructType(), (Struct) value);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to convert value to " + type.typeId().name() + ", field: " + name,
          e);
    }
  }

  private List<?> toList(
      org.apache.iceberg.types.Type type,
      String name,
      Object value,
      Schema schema) {
    try {
      var elementType = type.asListType().elementType();
      var result = new ArrayList<>();
      var index = 0;
      for (var v : (Collection<?>) value) {
        var castedValue = castValue(String.valueOf(index++), elementType, v, schema.valueSchema());
        result.add(castedValue);
      }
      return result;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to convert value to " + type.typeId().name() + ", field: " + name,
          e);
    }
  }

  private Map<?, ?> toMap(
      org.apache.iceberg.types.Type type,
      String name,
      Object value,
      Schema schema) {
    try {
      var keyType = type.asMapType().keyType();
      var valueType = type.asMapType().valueType();
      return ((Map<?, ?>) value).entrySet()
          .stream()
          .map(e -> {
            var key = castValue("key", keyType, e.getKey(), schema.keySchema());
            var val = castValue("value", valueType, e.getValue(), schema.valueSchema());
            return Pair.of(key, val);
          })
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to convert value to " + type.typeId().name() + ", field: " + name,
          e);
    }
  }

}
