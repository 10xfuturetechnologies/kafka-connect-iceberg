package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.converter;

import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.IcebergSinkConnectorConfig;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

public class SchemaConverter {

  private static final long MAX_SCHEMA_CACHE_SIZE = 1000;
  private static final long SCHEMA_CACHE_TTL_SEC = 10 * 60;

  private final AvroData avroData;
  private final LoadingCache<Schema, org.apache.iceberg.Schema> schemaCache;

  public SchemaConverter(IcebergSinkConnectorConfig config) {
    avroData = new AvroData(new AvroDataConfig(config.originals()));
    schemaCache = CacheBuilder.newBuilder()
        .maximumSize(MAX_SCHEMA_CACHE_SIZE)
        .ticker(Ticker.systemTicker())
        .expireAfterWrite(SCHEMA_CACHE_TTL_SEC, TimeUnit.SECONDS)
        .build(new CacheLoader<>() {
          @Override
          public org.apache.iceberg.Schema load(Schema kafkaConnectSchema) {
            // TODO find a more direct/efficient way to translate this
            var avroSchema = avroData.fromConnectSchema(kafkaConnectSchema);
            return AvroSchemaUtil.toIceberg(avroSchema);
          }
        });
  }

  public org.apache.iceberg.Schema convert(Schema kafkaConnectSchema) {
    try {
      return schemaCache.get(kafkaConnectSchema);
    } catch (ExecutionException e) {
      // this should not fail
      throw new ConnectException("Unable to convert schema " + kafkaConnectSchema, e);
    }
  }
}
