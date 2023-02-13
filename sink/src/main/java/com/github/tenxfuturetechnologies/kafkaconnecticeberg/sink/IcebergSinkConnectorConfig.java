package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink;

import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.iceberg.DefaultIcebergWriterFactory;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpecFactory;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.spi.IcebergWriterFactory;
import io.confluent.connect.storage.StorageSinkConnectorConfig.SchemaCompatibilityRecommender;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

public class IcebergSinkConnectorConfig extends AbstractConfig {

  // @formatter:off
  public static final String TIMEZONE_CONFIG = "timezone";
  public static final String TIMEZONE_DEFAULT = "";
  public static final String TIMEZONE_DOC =
      "Timezone used for date, time and timestamp conversion and partitioning. Defaults to the "
          + "system default timezone.";
  public static final String TIMEZONE_DISPLAY = "Timezone";
  private static final ConfigDef.Validator TIMEZONE_VALIDATOR = new ConfigDef.Validator() {
    @Override
    public void ensureValid(String name, Object value) {
      if (value == null) {
        value = TIMEZONE_DEFAULT;
      }
      if (StringUtils.isEmpty(value.toString())) {
        return;
      }
      try {
        ZoneId.of(value.toString());
      } catch (Exception e) {
        throw new ConfigException("Invalid timezone " + value, e);
      }
    }

    @Override
    public String toString() {
      return "[" + Utils.join(ZoneId.getAvailableZoneIds(), ", ") + "]";
    }
  };

  // Connector group
  public static final String FLUSH_SIZE_CONFIG = "flush.size";
  public static final int FLUSH_SIZE_DEFAULT = 100;
  public static final String FLUSH_SIZE_DOC =
      "Number of records written to store before invoking file commits.";
  public static final String FLUSH_SIZE_DISPLAY = "Flush Size";
  private static final ConfigDef.Validator FLUSH_SIZE_VALIDATOR = ConfigDef.Range.atLeast(1);

  public static final String FLUSH_WALL_CLOCK_ROTATE_INTERVAL_MS_CONFIG =
      "flush.wall-clock-rotate-interval-ms";
  // TODO for predictable data file content generation we need to default this to Long.MAX_VALUE
  // TODO and control flush intervals using cluster clock
  public static final long FLUSH_WALL_CLOCK_ROTATE_INTERVAL_MS_DEFAULT = 10 * 60 * 1000L;
  public static final String FLUSH_WALL_CLOCK_ROTATE_INTERVAL_MS_DOC =
      "Max wall-clock-millisecond ingestion interval before committing files.";
  public static final String FLUSH_WALL_CLOCK_ROTATE_INTERVAL_MS_DISPLAY =
      "Flush Wall Clock Interval MS";
  private static final ConfigDef.Validator FLUSH_WALL_CLOCK_ROTATE_INTERVAL_MS_VALIDATOR =
      ConfigDef.Range.atLeast(0);

  public static final String WRITER_FACTORY_CLASS_CONFIG = "writer.factory.class";
  public static final Class<? extends IcebergWriterFactory> WRITER_FACTORY_CLASS_DEFAULT =
      DefaultIcebergWriterFactory.class;
  public static final String WRITER_FACTORY_CLASS_DOC =
      "Factory class to create IcebergWriter instances";
  public static final String WRITER_FACTORY_CLASS_DISPLAY = "Iceberg Writer Factory Class";

  public static final String WRITER_KEEP_RECORD_FIELDS_CONFIG = "writer.keep-record-fields";
  public static final boolean WRITER_KEEP_RECORD_FIELDS_DEFAULT = false;
  public static final String WRITER_KEEP_RECORD_FIELDS_DOC =
      "Sink records to data files using the original record schema instead of the table schema";
  public static final String WRITER_KEEP_RECORD_FIELDS_DISPLAY =
      "Iceberg Writer Keep Record Fields";

  // Schema group
  public static final String SCHEMA_COMPATIBILITY_CONFIG = "schema.compatibility";
  public static final String SCHEMA_COMPATIBILITY_DOC =
      "The schema compatibility rule to use when the connector is observing schema changes. The "
          + "supported configurations are NONE, BACKWARD, FORWARD and FULL.";
  public static final String SCHEMA_COMPATIBILITY_DEFAULT = "NONE";
  public static final String SCHEMA_COMPATIBILITY_DISPLAY = "Schema Compatibility";
  public static final ConfigDef.Recommender SCHEMA_COMPATIBILITY_RECOMMENDER =
      new SchemaCompatibilityRecommender();

  // Table group
  public static final String TABLE_NAMESPACE_CONFIG = "table.namespace";
  public static final String TABLE_NAMESPACE_DEFAULT = "default";
  public static final String TABLE_NAMESPACE_DOC =
      "Table namespace. In Glue it will be used as database name";
  public static final String TABLE_NAMESPACE_DISPLAY = "Table Namespace";

  public static final String TABLE_NAME_CONFIG = "table.name";
  public static final String TABLE_NAME_DEFAULT = null;
  public static final String TABLE_NAME_DOC =
      "Name of the table where the data will be insert into. This is required.";
  public static final String TABLE_NAME_DISPLAY = "Table Name";

  public static final String TABLE_AUTO_CREATE_CONFIG = "table.auto-create";
  public static final boolean TABLE_AUTO_CREATE_DEFAULT = false;
  public static final String TABLE_AUTO_CREATE_DOC =
      "When true sink will automatically create new Iceberg tables";
  public static final String TABLE_AUTO_CREATE_DISPLAY = "Table Auto-create";

  public static final String TABLE_SCHEMA_EVOLVE_CONFIG = "table.schema.evolve";
  public static final boolean TABLE_SCHEMA_EVOLVE_DEFAULT = false;
  public static final String TABLE_SCHEMA_EVOLVE_DOC =
      "When true sink will be adding new columns to Iceberg tables on schema changes";
  public static final String TABLE_SCHEMA_EVOLVE_DISPLAY = "Allow Schema Evolution";

  public static final String TABLE_PRIMARY_KEY_CONFIG = "table.primary-key";
  public static final String TABLE_PRIMARY_KEY_DEFAULT = null;
  public static final String TABLE_PRIMARY_KEY_DOC =
      "Comma-separated list of primary key column names.";
  public static final String TABLE_PRIMARY_KEY_DISPLAY = "Primary Key";

  public static final String TABLE_PARTITION_COLUMN_NAME_CONFIG = "table.partition.column-name";
  public static final String TABLE_PARTITION_COLUMN_NAME_DEFAULT = null;
  public static final String TABLE_PARTITION_COLUMN_NAME_DOC =
      "Name of the column that will be used to partition the table. This is only used when the "
          + "table is auto-create for the first time and when building the data files.";
  public static final String TABLE_PARTITION_COLUMN_NAME_DISPLAY = "Table Partition Column";

  public static final String TABLE_PARTITION_TRANSFORM_CONFIG = "table.partition.transform";
  public static final String TABLE_PARTITION_TRANSFORM_DEFAULT = "void";
  public static final String TABLE_PARTITION_TRANSFORM_DOC =
      "Partition transform on the partition column value as per the Iceberg spec. This setting is "
          + "used only when the table is auto-created.";
  public static final String TABLE_PARTITION_TRANSFORM_DISPLAY = "Table Partition Transform";
  private static final ConfigDef.Validator TABLE_PARTITION_TRANSFORM_VALIDATOR =
      new ConfigDef.Validator() {
        private final PartitionTransformSpecFactory factory = new PartitionTransformSpecFactory();

        @Override

        public void ensureValid(String name, Object value) {
          if (value != null) {
            try {
              factory.create(value.toString());
            } catch (Exception e) {
              throw new ConfigException("Invalid partition transform", e);
            }
          }
        }
      };

  // Iceberg group
  public static final String ICEBERG_PREFIX = "iceberg.";

  public static final String CATALOG_NAME_CONFIG = ICEBERG_PREFIX + "name";
  public static final String CATALOG_NAME_DEFAULT = "default";
  public static final String CATALOG_NAME_DOC = "Name of the Iceberg catalog to use";
  public static final String CATALOG_NAME_DISPLAY = "Iceberg Catalog Name";

  public static final String CATALOG_IMPL_CONFIG = ICEBERG_PREFIX + "catalog-impl";
  public static final String CATALOG_IMPL_DEFAULT = null;
  public static final String CATALOG_IMPL_DOC =
      "Iceberg catalog implementation (Only one of iceberg.catalog-impl and iceberg.type "
          + "can be set to non null value at the same time";
  public static final String CATALOG_IMPL_DISPLAY = "Iceberg Catalog Implementation";

  public static final String CATALOG_TYPE_CONFIG = ICEBERG_PREFIX + "type";
  public static final String CATALOG_TYPE_DEFAULT = null;
  public static final String CATALOG_TYPE_DOC =
      "Iceberg catalog type (Only one of iceberg.catalog-impl and iceberg.type "
          + "can be set to non null value at the same time)";
  public static final String CATALOG_TYPE_DISPLAY = "Iceberg Catalog Type";
  // @formatter:on

  private final String name;
  private final ZoneId timezone;

  public IcebergSinkConnectorConfig(Map<String, String> originals) {
    this(newConfigDef(), originals);
  }

  public IcebergSinkConnectorConfig(ConfigDef definition, Map<String, String> originals) {
    super(definition, originals);
    var providedName = originals.get("name");
    name = providedName != null ? providedName : "iceberg-sink";
    var stringTimezone = originals.get(TIMEZONE_CONFIG);
    timezone = StringUtils.isEmpty(stringTimezone)
        ? ZoneId.systemDefault()
        : ZoneId.of(stringTimezone);
  }

  @SuppressWarnings({"Indentation"})
  public static ConfigDef newConfigDef() {
    var configDef = new ConfigDef();

    {
      final String group = "Connector";
      int orderInGroup = 0;

      configDef.define(
          TIMEZONE_CONFIG,
          Type.STRING,
          TIMEZONE_DEFAULT,
          TIMEZONE_VALIDATOR,
          Importance.HIGH,
          TIMEZONE_DOC,
          group,
          ++orderInGroup,
          Width.SHORT,
          TIMEZONE_DISPLAY);

      configDef.define(
          FLUSH_SIZE_CONFIG,
          Type.INT,
          FLUSH_SIZE_DEFAULT,
          FLUSH_SIZE_VALIDATOR,
          Importance.HIGH,
          FLUSH_SIZE_DOC,
          group,
          ++orderInGroup,
          Width.LONG,
          FLUSH_SIZE_DISPLAY
      );

      configDef.define(
          FLUSH_WALL_CLOCK_ROTATE_INTERVAL_MS_CONFIG,
          Type.LONG,
          FLUSH_WALL_CLOCK_ROTATE_INTERVAL_MS_DEFAULT,
          FLUSH_WALL_CLOCK_ROTATE_INTERVAL_MS_VALIDATOR,
          Importance.HIGH,
          FLUSH_WALL_CLOCK_ROTATE_INTERVAL_MS_DOC,
          group,
          ++orderInGroup,
          Width.LONG,
          FLUSH_WALL_CLOCK_ROTATE_INTERVAL_MS_DISPLAY
      );

      configDef.define(
          WRITER_FACTORY_CLASS_CONFIG,
          Type.CLASS,
          WRITER_FACTORY_CLASS_DEFAULT,
          Importance.HIGH,
          WRITER_FACTORY_CLASS_DOC,
          group,
          ++orderInGroup,
          Width.LONG,
          WRITER_FACTORY_CLASS_DISPLAY
      );

      configDef.define(
          WRITER_KEEP_RECORD_FIELDS_CONFIG,
          Type.BOOLEAN,
          WRITER_KEEP_RECORD_FIELDS_DEFAULT,
          Importance.HIGH,
          WRITER_KEEP_RECORD_FIELDS_DOC,
          group,
          ++orderInGroup,
          Width.SHORT,
          WRITER_KEEP_RECORD_FIELDS_DISPLAY
      );
    }

    {
      final String group = "Schema";
      int orderInGroup = 0;

      configDef.define(
          SCHEMA_COMPATIBILITY_CONFIG,
          Type.STRING,
          SCHEMA_COMPATIBILITY_DEFAULT,
          Importance.HIGH,
          SCHEMA_COMPATIBILITY_DOC,
          group,
          ++orderInGroup,
          Width.SHORT,
          SCHEMA_COMPATIBILITY_DISPLAY,
          SCHEMA_COMPATIBILITY_RECOMMENDER);
    }

    {
      final String group = "Table";
      int orderInGroup = 0;

      configDef.define(
          TABLE_NAMESPACE_CONFIG,
          Type.STRING,
          TABLE_NAMESPACE_DEFAULT,
          Importance.MEDIUM,
          TABLE_NAMESPACE_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          TABLE_NAMESPACE_DISPLAY);

      configDef.define(
          TABLE_NAME_CONFIG,
          Type.STRING,
          TABLE_NAME_DEFAULT,
          Importance.HIGH,
          TABLE_NAME_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          TABLE_NAME_DISPLAY);

      configDef.define(
          TABLE_AUTO_CREATE_CONFIG,
          Type.BOOLEAN,
          TABLE_AUTO_CREATE_DEFAULT,
          Importance.MEDIUM,
          TABLE_AUTO_CREATE_DOC,
          group,
          ++orderInGroup,
          Width.SHORT,
          TABLE_AUTO_CREATE_DISPLAY);

      configDef.define(
          TABLE_SCHEMA_EVOLVE_CONFIG,
          Type.BOOLEAN,
          TABLE_SCHEMA_EVOLVE_DEFAULT,
          Importance.LOW,
          TABLE_SCHEMA_EVOLVE_DOC,
          group,
          ++orderInGroup,
          Width.SHORT,
          TABLE_SCHEMA_EVOLVE_DISPLAY);

      configDef.define(
          TABLE_PRIMARY_KEY_CONFIG,
          Type.LIST,
          TABLE_PRIMARY_KEY_DEFAULT,
          Importance.HIGH,
          TABLE_PRIMARY_KEY_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          TABLE_PRIMARY_KEY_DISPLAY);

      configDef.define(
          TABLE_PARTITION_COLUMN_NAME_CONFIG,
          Type.STRING,
          TABLE_PARTITION_COLUMN_NAME_DEFAULT,
          Importance.HIGH,
          TABLE_PARTITION_COLUMN_NAME_DOC,
          group,
          ++orderInGroup,
          Width.LONG,
          TABLE_PARTITION_COLUMN_NAME_DISPLAY);

      configDef.define(
          TABLE_PARTITION_TRANSFORM_CONFIG,
          Type.STRING,
          TABLE_PARTITION_TRANSFORM_DEFAULT,
          TABLE_PARTITION_TRANSFORM_VALIDATOR,
          Importance.HIGH,
          TABLE_PARTITION_TRANSFORM_DOC,
          group,
          ++orderInGroup,
          Width.SHORT,
          TABLE_PARTITION_TRANSFORM_DISPLAY);
    }

    {
      final String group = "Iceberg";
      int orderInGroup = 0;

      configDef.define(
          CATALOG_NAME_CONFIG,
          Type.STRING,
          CATALOG_NAME_DEFAULT,
          Importance.MEDIUM,
          CATALOG_NAME_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          CATALOG_NAME_DISPLAY);

      configDef.define(
          CATALOG_IMPL_CONFIG,
          Type.CLASS,
          CATALOG_IMPL_DEFAULT,
          Importance.MEDIUM,
          CATALOG_IMPL_DOC,
          group,
          ++orderInGroup,
          Width.LONG,
          CATALOG_IMPL_DISPLAY);

      configDef.define(
          CATALOG_TYPE_CONFIG,
          Type.STRING,
          CATALOG_TYPE_DEFAULT,
          Importance.MEDIUM,
          CATALOG_TYPE_DOC,
          group,
          ++orderInGroup,
          Width.LONG,
          CATALOG_TYPE_DISPLAY);
    }
    return configDef;
  }

  public String getName() {
    return name;
  }

  public ZoneId getTimezone() {
    return timezone;
  }

  public int getFlushSize() {
    return getInt(FLUSH_SIZE_CONFIG);
  }

  public long getFlushWallClockRotateIntervalMs() {
    return getLong(FLUSH_WALL_CLOCK_ROTATE_INTERVAL_MS_CONFIG);
  }

  public String getSchemaCompatibility() {
    return getString(SCHEMA_COMPATIBILITY_CONFIG);
  }

  public Class<? extends IcebergWriterFactory> getWriterFactoryClass() {
    return (Class<? extends IcebergWriterFactory>) getClass(WRITER_FACTORY_CLASS_CONFIG);
  }

  public boolean isWriteFileWithRecordSchema() {
    return getBoolean(WRITER_KEEP_RECORD_FIELDS_CONFIG);
  }

  public Set<String> getTablePrimaryKey() {
    var pkList = getList(TABLE_PRIMARY_KEY_CONFIG);
    return pkList == null ? Set.of() : Set.copyOf(pkList);
  }

  public String getTableNamespace() {
    return getString(TABLE_NAMESPACE_CONFIG);
  }

  public String getTableName() {
    return getString(TABLE_NAME_CONFIG);
  }

  public boolean isTableAutoCreate() {
    return getBoolean(TABLE_AUTO_CREATE_CONFIG);
  }

  public boolean isTableSchemaEvolve() {
    return getBoolean(TABLE_SCHEMA_EVOLVE_CONFIG);
  }

  public Optional<String> getTablePartitionColumn() {
    return Optional.ofNullable(getString(TABLE_PARTITION_COLUMN_NAME_CONFIG));
  }

  public String getTablePartitionTransform() {
    return getString(TABLE_PARTITION_TRANSFORM_CONFIG);
  }

  public String getCatalogName() {
    return getString(CATALOG_NAME_CONFIG);
  }

  public Map<String, String> getIcebergConfiguration() {
    var config = new HashMap<String, String>();
    originals().entrySet()
        .stream()
        .filter(entry -> entry.getKey().startsWith(ICEBERG_PREFIX))
        .forEach(entry ->
            config.put(
                entry.getKey().substring(ICEBERG_PREFIX.length()),
                entry.getValue().toString()));
    return config;
  }
}
