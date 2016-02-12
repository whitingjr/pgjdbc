/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2003-2016, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */

package org.postgresql.core.v3;

import static org.postgresql.core.Oid.UNSPECIFIED;

import org.postgresql.core.NativeQuery;
import org.postgresql.core.Oid;
import org.postgresql.core.Utils;

import java.util.ArrayList;
import java.util.List;


/**
 * Purpose of this object is to support batched query re write behaviour.
 * Responsibility for tracking the batch size and implement the clean up of the
 * query fragments after the batch execute is complete. Intended to be used to
 * wrap a Query that is present in the batchStatements collection.
 *
 * @author Jeremy Whiting jwhiting@redhat.com
 *
 */
public class BatchedQueryDecorator extends SimpleQuery {

  private final List<Integer> originalPreparedTypes;
  private boolean isPreparedTypesSet;
  private Integer isParsed = 0;

  private byte[] batchedEncodedName;

  public BatchedQueryDecorator(NativeQuery query,
      ProtocolConnectionImpl protoConnection) {
    super(query, protoConnection);
    int paramCount = getBindPositions();

    originalPreparedTypes = new ArrayList<Integer>();
    List<Integer> types = getStatementTypes();
    if (types != null && !types.isEmpty()) {
      for ( Integer i : types ) {
        originalPreparedTypes.add(i);
      }
    } else {
      for ( int i = 1 ; i <= paramCount ; i += 1 ) {
        originalPreparedTypes.add(UNSPECIFIED);
      }
    }

    setStatementName(null);
  }

  /**
   * Reset the batched query for next use.
   */
  public void reset() {
    super.setStatementTypes(originalPreparedTypes);
    resetBatchedCount();
  }

  @Override
  public boolean isStatementReWritableInsert() {
    return true;
  }

  /**
   * The original meta data may need updating.
   */
  @Override
  public void setStatementTypes(List<Integer> types) {
    super.setStatementTypes(types);
    if (isOriginalStale(types)) {
      updateOriginal(types);
    }
  }

  /**
   * Get the statement types for all parameters in the batch.
   *
   * @return int an array of {@link org.postgresql.core.Oid} parameter types
   */
  @Override
  public List<Integer> getStatementTypes() {
    List<Integer> types = super.getStatementTypes();
    if (isOriginalStale(types)) {
      /*
       * Use opportunity to update originals if a ParameterDescribe has been
       * called.
       */
      updateOriginal(types);
    }
    return resizeTypes();
  }

  /**
   * Check fields and update if out of sync
   *
   * @return int an array of {@link org.postgresql.core.Oid} parameter types
   */
  private List<Integer> resizeTypes() {
    // provide types depending on batch size, which may vary
    int expected = getBindPositions();
    List<Integer> types = super.getStatementTypes();

    if (types == null) {
      types = new ArrayList<Integer>();
      fill(expected, types);
    }
    if (types.size() < expected) {
      fill(expected, types);
    }
    setStatementTypes(types);
    return types;
  }

  private void fill(int expected, List<Integer> types) {
    if (originalPreparedTypes.isEmpty()) {
      for (int i = 0; i < expected ; i += 1) {
        types.add(Oid.UNSPECIFIED);
      }
    } else {
      int count = (expected - types.size()) / originalPreparedTypes.size();
      for (int i = 0; i < count; i += 1) {
        types.addAll(originalPreparedTypes);
      }
    }
  }

  @Override
  boolean isPreparedFor(List<Integer> paramTypes) {
    resizeTypes();
    return isStatementParsed() && super.isPreparedFor(paramTypes);
  }

  @Override
  byte[] getEncodedStatementName() {
    if (batchedEncodedName == null) {
      String n = super.getStatementName();
      if (n != null) {
        batchedEncodedName = Utils.encodeUTF8(n);
      }
    }
    return batchedEncodedName;
  }

  @Override
  void setStatementName(String statementName) {
    if (statementName == null) {
      batchedEncodedName = null;
      super.setStatementName(null);
    } else {
      super.setStatementName(statementName);
      batchedEncodedName = Utils.encodeUTF8(statementName);
    }
  }

  /**
   * Detect when the vanilla prepared type meta data is out of date.
   *
   * @param preparedTypes
   *          meta data to compare with
   * @return boolean value indicating if internal type information needs
   *         updating
   */
  private boolean isOriginalStale(List<Integer> preparedTypes) {
    if (isPreparedTypesSet) {
      return false;
    }
    if (preparedTypes == null) {
      return false;
    }
    if (preparedTypes.size() == 0) {
      return false;
    }
    if (preparedTypes.size() < originalPreparedTypes.size()) {
      return false;
    }
    int maxPos = originalPreparedTypes.size() - 1;
    for (int pos = 0; pos <= maxPos; pos += 1) {
      if (originalPreparedTypes.get(pos) == UNSPECIFIED
          && preparedTypes.get(pos) != UNSPECIFIED) {
        return true;
      }
    }
    return false;
  }

  private void updateOriginal(List<Integer> preparedTypes) {
    if (preparedTypes == null) {
      return;
    }
    if (preparedTypes.size() == 0) {
      return;
    }
    isPreparedTypesSet = true;
    int maxPos = originalPreparedTypes.size() - 1;
    for (int pos = 0; pos <= maxPos; pos++) {
      if (preparedTypes.get(pos) != UNSPECIFIED) {
        if (originalPreparedTypes.get(pos) == UNSPECIFIED) {
          originalPreparedTypes.set(pos, preparedTypes.get(pos));
        }
      } else {
        isPreparedTypesSet = false;
      }
    }
  }

  private boolean isStatementParsed() {
    return isParsed.equals(getBindPositions());
  }

  /**
   * Method to receive notification of the parsed/prepared status of the
   * statement.
   *
   * @param prepared
   *          state
   */
  public void registerQueryParsedStatus(boolean prepared) {
    isParsed = getBindPositions();
  }

  @Override
  String getNativeSql() {
    // dynamically rebuild sql with parameters for each batch
    if (super.getNativeSql() == null) {
      return "";
    }
    int c = super.getNativeQuery().bindPositions.length;
    int bs = getBatchSize();
    String ns = super.getNativeSql();
    // ',' '$' * bs-1 + '(' ')' * bs-1  
    int l = ns.length() + (((c * 2) - 1) * (bs -1)) + ((bs - 1) * 3 );
    // numbers
    
    StringBuilder s = new StringBuilder(l).append(ns);
    for (int i = 2; i <= bs; i += 1) {
      s.append(",");
      int initial = ((i - 1) * c) + 1;
      s.append("($").append(initial);
      for (int p = 1; p < c; p += 1) {
        s.append(",$").append(initial + p);
      }
      s.append(")");
    }
    return s.toString();
  }

  @Override
  public String toString() {
    return getNativeSql();
  }
}
