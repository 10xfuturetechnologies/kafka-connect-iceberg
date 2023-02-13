![build](https://github.com/10xfuturetechnologies/kafka-connect-iceberg/actions/workflows/ci.yml/badge.svg?event=push)
![lcense](https://img.shields.io/github/license/10xfuturetechnologies/kafka-connect-iceberg.svg)

# Kafka Connect Iceberg sink

A Kafka Connect connector which uses the Iceberg APIs to write data directly into an Iceberg table.

Some supported features include:
  - Automatic table creation upon receiving the first event
  - Schema evolution
  - Support for partitioned and un-partitioned tables
  - At-least-once delivery
  - Transactional data appends


## Example

The following code snippet shows an example of how to create an Iceberg sink connector via `curl`

    # Create a file with the connector configuration
    cat << EOF >> connector-config.json
    {
      "name": "test-partitioned-table-sink",
      "config": {
        "connector.class": "com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.IcebergSinkConnector",
        "producer.interceptor.classes": "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor",
        "consumer.interceptor.classes": "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor",
        "topics.regex": "example-topic",
        "errors.log.enable": "true",
        "errors.retry.delay.max.ms": "60000",
        "errors.retry.timeout": "5",
        "format.class": "io.confluent.connect.s3.format.avro.AvroFormat",
        "header.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
        "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schemas.enable": "true",
        "value.converter.schema.registry.url": "https://schemaregistry.localdomain:8081",
        "key.converter.enhanced.avro.schema.support": true,
        "transforms": "TombstoneHandler",
        "transforms.TombstoneHandler.type": "io.confluent.connect.transforms.TombstoneHandler",
        "transforms.TombstoneHandler.behavior": "warn",
    
        "table.name": "test_partitioned_table",
        "table.auto-create": true,
        "table.partition.column-name": "event_timestamp",
        "table.partition.transform": "day",
        "timezone": "UTC",
        "flush.size": 1000,
        "iceberg.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "iceberg.warehouse": "s3a://my-bucket/iceberg",
        "iceberg.fs.defaultFS": "s3a://my-bucket/iceberg",
        "iceberg.com.amazonaws.services.s3.enableV4": true,
        "iceberg.com.amazonaws.services.s3a.enableV4": true,
        "iceberg.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        "iceberg.fs.s3a.path.style.access": true,
        "iceberg.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
      }
    }
    EOF
    
    # Create the connector
    curl -X POST \
         -H "Content-Type: application/json" \
         -d @connector-config.json \
         https://kafkaconnect.localdomain/connectors/
    
    # Check the status of the connector
    curl https://kafkaconnect.localdomain/connectors/test-partitioned-table-sink/status | jq


## Credits

We thank the developers of [kafka-connector-iceberg-sink](https://github.com/getindata/kafka-connect-iceberg-sink) since this project as served as an inspiration to write this code.

We also thank [10X Banking](https://www.10xbanking.com/) for letting us open source the code.


