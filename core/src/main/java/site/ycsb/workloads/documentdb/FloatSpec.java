package site.ycsb.workloads.documentdb;

import com.google.auto.value.AutoValue;

/**
 * Spec for a floating point number.
 */
@AutoValue
public abstract class FloatSpec {

  public abstract double start();

  public abstract double end();
}
