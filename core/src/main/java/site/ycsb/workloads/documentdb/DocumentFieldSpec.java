package site.ycsb.workloads.documentdb;

import com.google.auto.value.AutoOneOf;

/** Spec for a document field. */
@AutoOneOf(DocumentFieldSpec.Type.class)
public abstract class DocumentFieldSpec {
  enum Type {
    INTEGRAL, FLOATING_POINT, STRING, ARRAY
  }

  public abstract Type type();

  public abstract IntegralSpec integral();

  public abstract FloatSpec floatingPoint();

  public abstract StringSpec string();

  public abstract ArraySpec array();
}

