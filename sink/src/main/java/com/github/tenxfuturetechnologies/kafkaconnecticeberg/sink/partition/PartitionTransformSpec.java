package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition;

import static java.util.regex.Pattern.CASE_INSENSITIVE;

import java.util.regex.Pattern;
import org.apache.iceberg.PartitionSpec.Builder;

public interface PartitionTransformSpec {

  Builder apply(Builder partitionSpecBuild, String columnName);


  class IdentityPartitionTransformSpec implements PartitionTransformSpec {

    @Override
    public Builder apply(Builder partitionSpecBuild, String columnName) {
      return partitionSpecBuild.identity(columnName);
    }
  }

  class BucketPartitionTransformSpec implements PartitionTransformSpec {

    private static final Pattern BUCKET_REGEX =
        Pattern.compile("bucket\\[(\\d+)\\]", CASE_INSENSITIVE);

    private final int numOfBuckets;

    protected BucketPartitionTransformSpec(int numOfBuckets) {
      this.numOfBuckets = numOfBuckets;
    }

    public static BucketPartitionTransformSpec parse(String spec) {
      var matcher = BUCKET_REGEX.matcher(spec);
      if (!matcher.matches()) {
        throw new IllegalArgumentException(
            "Spec '" + spec + "' does not match pattern " + BUCKET_REGEX);
      }
      var numOfBuckets = Integer.valueOf(matcher.group(1));
      return new BucketPartitionTransformSpec(numOfBuckets);
    }

    @Override
    public Builder apply(Builder partitionSpecBuild, String columnName) {
      return partitionSpecBuild.bucket(columnName, numOfBuckets);
    }

    // Visible for testing
    int getNumOfBuckets() {
      return numOfBuckets;
    }
  }

  class TruncatePartitionTransformSpec implements PartitionTransformSpec {

    private static final Pattern TRUNCATE_REGEX =
        Pattern.compile("truncate\\[(\\d+)\\]", CASE_INSENSITIVE);

    private final int width;

    protected TruncatePartitionTransformSpec(int width) {
      this.width = width;
    }

    public static TruncatePartitionTransformSpec parse(String spec) {
      var matcher = TRUNCATE_REGEX.matcher(spec);
      if (!matcher.matches()) {
        throw new IllegalArgumentException(
            "Spec '" + spec + "' does not match pattern " + TRUNCATE_REGEX);
      }
      var width = Integer.valueOf(matcher.group(1));
      return new TruncatePartitionTransformSpec(width);
    }

    @Override
    public Builder apply(Builder partitionSpecBuild, String columnName) {
      return partitionSpecBuild.truncate(columnName, width);
    }

    // Visible for testing
    int getWidth() {
      return width;
    }
  }


  class YearPartitionTransformSpec implements PartitionTransformSpec {

    @Override
    public Builder apply(Builder partitionSpecBuild, String columnName) {
      return partitionSpecBuild.year(columnName);
    }
  }

  class MonthPartitionTransformSpec implements PartitionTransformSpec {

    @Override
    public Builder apply(Builder partitionSpecBuild, String columnName) {
      return partitionSpecBuild.month(columnName);
    }
  }

  class DayPartitionTransformSpec implements PartitionTransformSpec {

    @Override
    public Builder apply(Builder partitionSpecBuild, String columnName) {
      return partitionSpecBuild.day(columnName);
    }
  }

  class HourPartitionTransformSpec implements PartitionTransformSpec {

    @Override
    public Builder apply(Builder partitionSpecBuild, String columnName) {
      return partitionSpecBuild.hour(columnName);
    }
  }


  class VoidPartitionTransformSpec implements PartitionTransformSpec {

    @Override
    public Builder apply(Builder partitionSpecBuild, String columnName) {
      return partitionSpecBuild;
    }
  }
}
