package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.BucketPartitionTransformSpec;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.DayPartitionTransformSpec;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.HourPartitionTransformSpec;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.IdentityPartitionTransformSpec;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.MonthPartitionTransformSpec;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.TruncatePartitionTransformSpec;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.VoidPartitionTransformSpec;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.YearPartitionTransformSpec;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class PartitionTransformSpecFactoryTest {

  private PartitionTransformSpecFactory underTest = new PartitionTransformSpecFactory();

  public static Stream<Arguments> validPartitionTransforms() {
    return Stream.of(
        Arguments.of("identity", IdentityPartitionTransformSpec.class),
        Arguments.of("bucket[3]", BucketPartitionTransformSpec.class),
        Arguments.of("truncate[10]", TruncatePartitionTransformSpec.class),
        Arguments.of("year", YearPartitionTransformSpec.class),
        Arguments.of("month", MonthPartitionTransformSpec.class),
        Arguments.of("day", DayPartitionTransformSpec.class),
        Arguments.of("hour", HourPartitionTransformSpec.class),
        Arguments.of("void", VoidPartitionTransformSpec.class)
    );
  }

  @ParameterizedTest
  @MethodSource("validPartitionTransforms")
  void shouldCreatePartitionTransformSpec(String spec, Class<?> clazz) {
    var pts = underTest.create(spec);

    assertThat(pts).isInstanceOf(clazz);
  }

  @ParameterizedTest
  @ValueSource(strings = {"+", "[null]"})
  void shouldThrowErrorWhenPartitionTransformSpecStringDoesNotMatchFormat(String spec) {
    var e = assertThrows(IllegalArgumentException.class, () -> underTest.create(spec));

    assertThat(e).hasMessageStartingWith("Unknown Iceberg partition transform for value ");
  }

  @ParameterizedTest
  @ValueSource(strings = {"abc", "null", "unknown", "invalid[2]"})
  void shouldThrowErrorWhenPartitionTransformSpecStringIsInvalid(String spec) {
    var e = assertThrows(IllegalArgumentException.class, () -> underTest.create(spec));

    assertThat(e).hasMessageStartingWith("Unknown Iceberg partition transform ");
  }
}