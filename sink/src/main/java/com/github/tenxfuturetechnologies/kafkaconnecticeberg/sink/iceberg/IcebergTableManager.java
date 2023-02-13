package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.iceberg;

import static java.lang.String.format;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;

import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.IcebergSinkConnectorConfig;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpecFactory;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergTableManager {

  private static final Logger log = LoggerFactory.getLogger(IcebergTableManager.class);

  private final IcebergSinkConnectorConfig config;
  private final Catalog catalog;
  private final PartitionTransformSpecFactory partitionTransformSpecFactory;
  private Table table;

  public IcebergTableManager(IcebergSinkConnectorConfig config, Catalog catalog,
      PartitionTransformSpecFactory partitionTransformSpecFactory) {
    this.config = config;
    this.catalog = catalog;
    this.partitionTransformSpecFactory = partitionTransformSpecFactory;
  }

  public Table from(final Schema schema) {
    var tableSchema = addPrimaryKeyIfNeeded(schema);
    if (table == null) {
      var tableId = TableIdentifier.of(Namespace.of(config.getTableNamespace()),
          config.getTableName());
      table = loadTable(catalog, tableId).orElseGet(() -> {
        if (!config.isTableAutoCreate()) {
          var message = format(
              "Table '%s' not found! Set '%s' to true to create tables automatically!",
              tableId,
              IcebergSinkConnectorConfig.TABLE_AUTO_CREATE_CONFIG);
          throw new ConnectException(message);
        }
        return createTable(tableId, tableSchema);
      });
    }
    if (config.isTableSchemaEvolve()) {
      evolveSchemaIfNeeded(tableSchema);
    }
    return table;
  }

  public Schema mergeTableSchema(Schema schema) {
    if (table.schema().equals(schema)) {
      return table.schema();
    }
    // TODO how non-performant is this?
    var transaction = table.newTransaction();
    var updatedSchema = transaction.updateSchema()
        .unionByNameWith(schema)
        .setIdentifierFields(schema.identifierFieldNames())
        .apply();
    table.refresh();
    return updatedSchema;
  }

  private Schema addPrimaryKeyIfNeeded(final Schema schema) {
    var tableSchema = schema;
    var primaryKeyColumn = config.getTablePrimaryKey();
    if (!primaryKeyColumn.isEmpty()) {
      log.info(
          "Configuring table '{}' with primary key {}",
          config.getTableName(),
          primaryKeyColumn);
      var primaryKeyColumIds = primaryKeyColumn.stream()
          .map(cn -> schema.findField(cn).fieldId())
          .collect(Collectors.toSet());
      tableSchema = new Schema(schema.columns(), primaryKeyColumIds);
    }
    return tableSchema;
  }

  private Table createTable(TableIdentifier tableIdentifier, final Schema schema) {
    log.info(
        "Creating table:'{}'\nschema:{}\nrowIdentifier:{}",
        tableIdentifier.name(),
        schema,
        schema.identifierFieldNames());

    final PartitionSpec ps;
    if (config.getTablePartitionColumn().isPresent()) {
      log.info(
          "Table '{}' will be partitioned by '{}' with transform '{}'",
          tableIdentifier.name(),
          config.getTablePartitionColumn().get(),
          config.getTablePartitionTransform());
      ps = partitionTransformSpecFactory.create(config.getTablePartitionTransform())
          .apply(PartitionSpec.builderFor(schema), config.getTablePartitionColumn().get())
          .build();
    } else {
      log.info("Table '{}' will be un-partitioned", tableIdentifier);
      ps = PartitionSpec.builderFor(schema).build();
    }
    log.trace("Partition spec is {}", ps.toString());

    return catalog.buildTable(tableIdentifier, schema)
        .withProperties(config.getIcebergConfiguration())
        .withProperty(FORMAT_VERSION, "2")
        .withSortOrder(getIdentifierFieldsAsSortOrder(schema))
        .withPartitionSpec(ps)
        .create();
  }

  private SortOrder getIdentifierFieldsAsSortOrder(Schema schema) {
    SortOrder.Builder sob = SortOrder.builderFor(schema);
    for (String fieldName : schema.identifierFieldNames()) {
      sob = sob.asc(fieldName);
    }

    return sob.build();
  }

  private void evolveSchemaIfNeeded(Schema schema) {
    IcebergUtils.transactional(() -> {
      if (isSameSchema(schema)) {
        log.trace("Schema of '{}' will not change", table.name());
        return;
      }

      table.refresh();
      var transaction = table.newTransaction();
      var updatedSchema = transaction.updateSchema()
              .unionByNameWith(schema)
              .setIdentifierFields(schema.identifierFieldNames());
      var newSchema = updatedSchema.apply();

      // Double check avoid committing when there is no schema change. commit creates new commit
      // even when there is no change!
      if (!table.schema().sameSchema(newSchema)) {
        log.info("Extending schema of {}", table.name());
        updatedSchema.commit();
        transaction.commitTransaction();
        log.debug("New schema of {} is {}", table.name(), newSchema);
      }
    });
  }

  private boolean isSameSchema(Schema schema) {
    if (!table.schema().sameSchema(schema)) {
      table.refresh();
    }
    return table.schema().sameSchema(schema);
  }

  private Optional<Table> loadTable(Catalog icebergCatalog, TableIdentifier tableId) {
    try {
      var table = icebergCatalog.loadTable(tableId);
      return Optional.of(table);
    } catch (NoSuchTableException e) {
      log.info("Table not found: {}", tableId.toString());
      return Optional.empty();
    }
  }
}
