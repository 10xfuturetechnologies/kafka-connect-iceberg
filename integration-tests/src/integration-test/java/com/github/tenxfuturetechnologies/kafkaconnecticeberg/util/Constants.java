package com.github.tenxfuturetechnologies.kafkaconnecticeberg.util;

public final class Constants {

  public static final String S3_ACCESS_KEY = "admin";
  public static final String S3_SECRET_KEY = "12345678";
  public static final String S3_BUCKET = "test-bucket";
  public static final String S3_REGION_NAME = "eu-west-1";
  public static final String TABLE_NAMESPACE = "iceberg_ns";
  public static final String TABLE_NAME = "test_table";
  public static final String MINIO_IMAGE = "minio/minio:latest";
  public static final String KAFKA_IMAGE = "confluentinc/cp-kafka:7.2.2";
  public static final String KAFKA_CONNECT_IMAGE = "confluentinc/cp-kafka-connect:7.2.2";
  public static final String SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:7.2.2";

  private Constants() {
  }
}
