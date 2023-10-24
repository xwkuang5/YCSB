package site.ycsb.workloads.documentdb;

import com.google.auto.value.AutoValue;

/**
 * Spec for a size range.
 */
@AutoValue
public abstract class SizeRangeSpec {

  public abstract int min();

  public abstract int max();
}
