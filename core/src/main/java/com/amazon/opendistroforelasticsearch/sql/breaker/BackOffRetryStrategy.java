package com.amazon.opendistroforelasticsearch.sql.breaker;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class BackOffRetryStrategy {

  /** Interval (ms) between each retry. */
  private static final long[] intervals = milliseconds(new double[] {4, 8 + 4, 16 + 4});

  /** Delta to randomize interval (ms). */
  private static final long delta = 4 * 1000;

  private static final int threshold = 85;

  private static AtomicLong mem = new AtomicLong(0L);

  private static final Object obj = new Object();

  public static final Supplier<Integer> GET_CB_STATE = () -> isMemoryHealthy() ? 0 : 1;

  private BackOffRetryStrategy() {

  }

  /** Is Healthy. */
  public static boolean isHealthy() {
    for (int i = 0; i < intervals.length; i++) {
      if (isMemoryHealthy()) {
        return true;
      }

      log.warn(
          "[MCB1] Memory monitor is unhealthy now, back off retrying: {} attempt, thread id = {}",
          i,
          Thread.currentThread().getId());
      if (ThreadLocalRandom.current().nextBoolean()) {
        log.warn("[MCB1] Directly abort on idx {}.", i);
        return false;
      }
      backOffSleep(intervals[i]);
    }

    boolean isHealthy = isMemoryHealthy();

    return isHealthy;
  }

  /** BackOff Sleep. */
  public static void backOffSleep(long interval) {
    try {
      long millis = randomize(interval);

      log.info("[MCB] Back off sleeping: {} ms", millis);
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      log.error("[MCB] Sleep interrupted", e);
    }
  }

  /** Generate random interval in [interval-delta, interval+delta). */
  private static long randomize(long interval) {
    // Random number within range generator for JDK 7+
    return ThreadLocalRandom.current().nextLong(lowerBound(interval), upperBound(interval));
  }

  private static long lowerBound(long interval) {
    return Math.max(0, interval - delta);
  }

  private static long upperBound(long interval) {
    return interval + delta;
  }

  private static long[] milliseconds(double[] seconds) {
    return Arrays.stream(seconds).mapToLong((second) -> (long) (1000 * second)).toArray();
  }

  private static boolean isMemoryHealthy() {
    final long freeMemory = Runtime.getRuntime().freeMemory();
    final long totalMemory = Runtime.getRuntime().totalMemory();
    final int memoryUsage =
        (int)
            Math.round(
                (double) (totalMemory - freeMemory + mem.get()) / (double) totalMemory * 100);

    log.debug(
        "[MCB1] Memory total, free, allocate: {}, {}, {}", totalMemory, freeMemory, mem.get());
    log.debug("[MCB1] Memory usage and limit: {}%, {}%", memoryUsage, threshold);

    return memoryUsage < threshold;
  }
}
