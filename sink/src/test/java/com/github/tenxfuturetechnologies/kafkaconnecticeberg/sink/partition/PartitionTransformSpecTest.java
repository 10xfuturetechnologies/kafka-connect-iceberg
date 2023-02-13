package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.BucketPartitionTransformSpec;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.DayPartitionTransformSpec;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.HourPartitionTransformSpec;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.IdentityPartitionTransformSpec;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.MonthPartitionTransformSpec;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.TruncatePartitionTransformSpec;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.VoidPartitionTransformSpec;
import com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.partition.PartitionTransformSpec.YearPartitionTransformSpec;
import org.apache.iceberg.PartitionSpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PartitionTransformSpecTest {

  private @Mock PartitionSpec.Builder builder;

  @Test
  void shouldApplyIdentityPartitionTransformSpec() {
    new IdentityPartitionTransformSpec().apply(builder, "col");

    verify(builder).identity("col");
    verifyNoMoreInteractions(builder);
  }

  @Test
  void shouldApplyBucketPartitionTransformSpec() {
    new BucketPartitionTransformSpec(4).apply(builder, "col");

    verify(builder).bucket("col", 4);
    verifyNoMoreInteractions(builder);
  }

  @Test
  void shouldParseBucketPartitionTransformSpec() {
    var pts = BucketPartitionTransformSpec.parse("bucket[7]");

    assertThat(pts).isNotNull().isInstanceOf(BucketPartitionTransformSpec.class)
        .satisfies(bpts -> assertThat(bpts.getNumOfBuckets()).isEqualTo(7));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "invalid", "bucket", "bucket[1]+", "bucket(1)", "bucket[1)",
      "bucket+[1]"})
  void shouldThrowExceptionWhenParsingInvalidBucketPartitionTransformSpec(String spec) {
    var e = assertThrows(IllegalArgumentException.class,
        () -> BucketPartitionTransformSpec.parse(spec));

    assertThat(e).hasMessage("Spec '" + spec + "' does not match pattern bucket\\[(\\d+)\\]");
  }

  @Test
  void shouldApplyTruncatePartitionTransformSpec() {
    new TruncatePartitionTransformSpec(9).apply(builder, "col");

    verify(builder).truncate("col", 9);
    verifyNoMoreInteractions(builder);
  }

  @Test
  void shouldParseTruncatePartitionTransformSpec() {
    var pts = TruncatePartitionTransformSpec.parse("truncate[11]");

    assertThat(pts).isNotNull().isInstanceOf(TruncatePartitionTransformSpec.class)
        .satisfies(tpts -> assertThat(tpts.getWidth()).isEqualTo(11));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "invalid", "truncate", "truncate[1]+", "truncate(1)", "truncate[1)",
      "truncate+[1]"})
  void shouldThrowExceptionWhenParsingInvalidTruncatePartitionTransformSpec(String spec) {
    var e = assertThrows(IllegalArgumentException.class,
        () -> TruncatePartitionTransformSpec.parse(spec));

    assertThat(e).hasMessage("Spec '" + spec + "' does not match pattern truncate\\[(\\d+)\\]");
  }

  @Test
  void shouldApplyYearPartitionTransformSpec() {
    new YearPartitionTransformSpec().apply(builder, "col");

    verify(builder).year("col");
    verifyNoMoreInteractions(builder);
  }

  @Test
  void shouldApplyMonthPartitionTransformSpec() {
    new MonthPartitionTransformSpec().apply(builder, "col");

    verify(builder).month("col");
    verifyNoMoreInteractions(builder);
  }

  @Test
  void shouldApplyDayPartitionTransformSpec() {
    new DayPartitionTransformSpec().apply(builder, "col");

    verify(builder).day("col");
    verifyNoMoreInteractions(builder);
  }

  @Test
  void shouldApplyHourPartitionTransformSpec() {
    new HourPartitionTransformSpec().apply(builder, "col");

    verify(builder).hour("col");
    verifyNoMoreInteractions(builder);
  }

  @Test
  void shouldApplyVoidPartitionTransformSpec() {
    new VoidPartitionTransformSpec().apply(builder, "col");

    verifyNoInteractions(builder);
  }
}