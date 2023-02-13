package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition;

import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.BucketPartitionTransformSpec;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.DayPartitionTransformSpec;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.HourPartitionTransformSpec;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.IdentityPartitionTransformSpec;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.MonthPartitionTransformSpec;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.TruncatePartitionTransformSpec;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.VoidPartitionTransformSpec;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.YearPartitionTransformSpec;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Partition value transformer factory as per https://iceberg.apache.org/spec/#partition-transforms
 */
public class PartitionTransformSpecFactory {

  private static final Pattern TRANSFORM_NAME_PATTERN = Pattern.compile("(\\w+).*");

  public PartitionTransformSpec create(String value) {
    var nameMatcher = TRANSFORM_NAME_PATTERN.matcher(value);
    if (!nameMatcher.matches()) {
      throw new IllegalArgumentException("Unknown Iceberg partition transform for value " + value);
    }
    var transformName = nameMatcher.group(1).toLowerCase(Locale.ROOT);
    PartitionTransformSpec partitionTransformSpec = null;
    switch (transformName) {
      case "identity":
        partitionTransformSpec = new IdentityPartitionTransformSpec();
        break;
      case "bucket":
        partitionTransformSpec = BucketPartitionTransformSpec.parse(value);
        break;
      case "truncate":
        partitionTransformSpec = TruncatePartitionTransformSpec.parse(value);
        break;
      case "year":
        partitionTransformSpec = new YearPartitionTransformSpec();
        break;
      case "month":
        partitionTransformSpec = new MonthPartitionTransformSpec();
        break;
      case "day":
        partitionTransformSpec = new DayPartitionTransformSpec();
        break;
      case "hour":
        partitionTransformSpec = new HourPartitionTransformSpec();
        break;
      case "void":
        partitionTransformSpec = new VoidPartitionTransformSpec();
        break;
      default:
        throw new IllegalArgumentException("Unknown Iceberg partition transform " + transformName);
    }
    return partitionTransformSpec;
  }
}
