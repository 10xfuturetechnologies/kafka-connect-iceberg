package com.github.tenxfuturetechnologies.kafkaconnecticeberg.util;

import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.S3_ACCESS_KEY;
import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.S3_BUCKET;
import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.S3_REGION_NAME;
import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.S3_SECRET_KEY;
import static com.github.tenxfuturetechnologies.kafkaconnecticeberg.util.Constants.TABLE_NAMESPACE;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public final class SparkTestHelper {

  private final SparkSession spark;

  public SparkTestHelper(String s3url) {
    SparkConf sparkconf = new SparkConf()
        .setAppName("Iceberg-Reader")
        .setMaster("local[2]")
        .set("spark.ui.enabled", "false")
        .set("spark.eventLog.enabled", "false")
        .set("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .set("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .set("spark.hadoop.fs.s3a.endpoint", s3url)
        .set("spark.hadoop.fs.s3a.endpoint.region", S3_REGION_NAME)
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .set("spark.sql.catalog.spark_catalog.type", "hadoop")
        .set("spark.sql.catalog.spark_catalog.warehouse",
                "s3a://" + S3_BUCKET + "/iceberg_warehouse")
        .set("spark.sql.warehouse.dir", "s3a://" + S3_BUCKET + "/iceberg_warehouse")
        .set("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .set("spark.sql.iceberg.handle-timestamp-without-timezone", "true");

    spark = SparkSession
        .builder()
        .config(sparkconf)
        .getOrCreate();
  }

  public Dataset<Row> query(String stmt) {
    return spark.newSession().sql(stmt);
  }

  public void dropTable(String table) {
    spark.newSession().sql("DROP TABLE IF EXISTS " + fullyQualifiedTableName(table));
  }

  public Dataset<Row> getTableData(String table) {
    return query(
        "SELECT *, input_file_name() as input_file FROM " + fullyQualifiedTableName(table));
  }

  public String fullyQualifiedTableName(String table) {
    return String.format("%s.%s", TABLE_NAMESPACE, table);
  }
}
