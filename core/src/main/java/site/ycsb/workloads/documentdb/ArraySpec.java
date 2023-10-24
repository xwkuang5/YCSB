package site.ycsb.workloads.documentdb;

import com.google.auto.value.AutoValue;

/**
 * Spec for an array.
 */
@AutoValue
public abstract class ArraySpec {

  public abstract SizeRangeSpec sizeRangeSpec();

  public abstract DocumentFieldSpec documentFieldSpec();
}
