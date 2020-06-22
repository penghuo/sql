package com.amazon.opendistroforelasticsearch.sql.breaker;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;

/**
 * The Aspect of {@link MemCircuitBreaker}.
 */
@Aspect
public class MemCircuitBreakerAspect {

  /** memory healthy check. */
  @Before("@annotation(com.amazon.opendistroforelasticsearch.sql.breaker.MemCircuitBreaker)")
  public void memoryHealthy(JoinPoint joinPoint) throws Throwable {
    if (!BackOffRetryStrategy.isHealthy()) {
      throw new IllegalStateException("Heap Memory Is Low");
    }
  }
}
