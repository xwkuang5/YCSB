package site.ycsb.workloads.documentdb;

import com.google.auto.value.AutoValue;

/**
 * Spec for a string.
 */
@AutoValue
public abstract class StringSpec {

  public abstract SizeRangeSpec sizeRangeSpec();
}
