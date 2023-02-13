package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.iceberg;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IcebergUtils {
  private static final Logger log = LoggerFactory.getLogger(IcebergUtils.class);
  private static final long MAX_BACKOFF = TimeUnit.SECONDS.toMillis(64);
  private static final int MAX_RETRIES = 6;

  private IcebergUtils() {}

  public static void transactional(Runnable operation) {
    transactional(() -> {
      operation.run();
      return null;
    });
  }

  public static <T> T transactional(Supplier<T> operation) {
    int retries = 0;
    while (retries <= Integer.MAX_VALUE) {
      try {
        return operation.get();
      } catch (CommitFailedException e) {
        if (retries >= MAX_RETRIES - 1) {
          throw e;
        }
        log.debug("Got commit exception", e);
        sleep(retries++);
      }
    }
    throw new IllegalStateException("This should not happen");
  }

  private static void sleep(int retryNumber) {
    var rand = Math.random() * 1000L;
    var sleepDuration = (long) Math.min(Math.pow(2, retryNumber) * rand, MAX_BACKOFF);
    try {
      Thread.sleep(sleepDuration);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted whilst sleeping");
    }
  }
}
