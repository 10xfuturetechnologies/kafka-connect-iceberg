package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.iceberg;

import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.IcebergSinkConnectorConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;

public class IcebergCatalogFactory {

  public static Catalog create(IcebergSinkConnectorConfig config) {
    var hadoopConfig = new Configuration();
    var catalogConfiguration = config.getIcebergConfiguration();
    catalogConfiguration.forEach(hadoopConfig::set);
    return CatalogUtil.buildIcebergCatalog(
        config.getCatalogName(),
        catalogConfiguration,
        hadoopConfig);
  }
}
