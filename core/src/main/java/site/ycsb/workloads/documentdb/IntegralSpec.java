package site.ycsb.workloads.documentdb;

import com.google.auto.value.AutoValue;

/**
 * Spec for integral numbers.
 */
@AutoValue
public abstract class IntegralSpec {

  public abstract long start();

  public abstract long end();
}
