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
import org.postgresql.core.Utils;

import java.util.Arrays;


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

  private final int[] originalPreparedTypes;
  private boolean isPreparedTypesSet;
  private Integer isParsed = 0;

  private byte[] batchedEncodedName;

  public BatchedQueryDecorator(NativeQuery query,
      ProtocolConnectionImpl protoConnection) {
    super(query, protoConnection);
    int paramCount = getBindPositions();

    originalPreparedTypes = new int[paramCount];
    if (getStatementTypes() != null && getStatementTypes().length > 0) {
      System.arraycopy(getStatementTypes(), 0, originalPreparedTypes, 0,
          paramCount);
    } else {
      Arrays.fill(originalPreparedTypes, UNSPECIFIED);
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
  public void setStatementTypes(int[] types) {
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
  public int[] getStatementTypes() {
    int types[] = super.getStatementTypes();
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
  private int[] resizeTypes() {
    // provide types depending on batch size, which may vary
    int expected = getBindPositions();
    int[] types = super.getStatementTypes();
    int before = 0;

    if (types == null) {
      types = fill(expected);
    }
    if (types.length < expected) {
      before = types.length;
      types = fill(expected);
    }
    if (before != types.length) {
      setStatementTypes(types);
    }
    return types;
  }

  private int[] fill(int expected) {
    int[] types = Arrays.copyOf(originalPreparedTypes, expected);
    for (int row = 1; row < getBatchSize(); row += 1) {
      System.arraycopy(originalPreparedTypes, 0, types, row
          * originalPreparedTypes.length, originalPreparedTypes.length);
    }
    return types;
  }

  @Override
  boolean isPreparedFor(int[] paramTypes) {
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
  private boolean isOriginalStale(int[] preparedTypes) {
    if (isPreparedTypesSet) {
      return false;
    }
    if (preparedTypes == null) {
      return false;
    }
    if (preparedTypes.length == 0) {
      return false;
    }
    if (preparedTypes.length < originalPreparedTypes.length) {
      return false;
    }
    int maxPos = originalPreparedTypes.length - 1;
    for (int pos = 0; pos <= maxPos; pos += 1) {
      if (originalPreparedTypes[pos] == UNSPECIFIED
          && preparedTypes[pos] != UNSPECIFIED) {
        return true;
      }
    }
    return false;
  }

  private void updateOriginal(int[] preparedTypes) {
    if (preparedTypes == null) {
      return;
    }
    if (preparedTypes.length == 0) {
      return;
    }
    isPreparedTypesSet = true;
    int maxPos = originalPreparedTypes.length - 1;
    for (int pos = 0; pos <= maxPos; pos++) {
      if (preparedTypes[pos] != UNSPECIFIED) {
        if (originalPreparedTypes[pos] == UNSPECIFIED) {
          originalPreparedTypes[pos] = preparedTypes[pos];
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
    // dynamically build sql with parameters for each batch
    if (super.getNativeSql() == null) {
      return "";
    }
    int c = super.getNativeQuery().bindPositions.length;
    int bs = getBatchSize();
    int l = getSize(super.getNativeSql().length(), c, bs - 1 );
    StringBuilder s = new StringBuilder(l).append(super.getNativeSql());
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

  /**
   * Calculate the size of the statement
   * @param init int Length of sql supplied by user
   * @param p int Number of parameters in a batch
   * @param remaining int Remaining batches to process
   * @return int Size of generated sql
   */
  private int getSize(int init, int p, int remaining) {
    int size = init;
    int total = p * remaining; // assuming native sql has a whole batch
    size += (2 * remaining); // bracket
    size += remaining + ((p - 1) * remaining); // parameter comma
    size += total; // dollar
    if (total > 999999999) {
      int params = Integer.MAX_VALUE - 999999999;
      size += (params * 10);
      total -= params;
    }
    if (total > 99999999) {
      int params = total - 99999999;
      size += (params * 9);
      total -= params;
    }
    if (total > 9999999) {
      int params = total - 9999999;
      size += (params * 8);
      total -= params;
    }
    if (total > 999999) {
      int params = total - 999999;
      size += (params * 7);
      total -= params;
    }
    if (total > 99999) {
      int params = total - 99999;
      size += (params * 6);
      total -= params;
    }
    if (total > 9999) {
      int params = total - 9999;
      size += (params * 5);
      total -= params;
    }
    if (total > 999) {
      int params = total - 999;
      size += (params * 4);
      total -= params;
    }
    if (total > 99) {
      int params = total - 99;
      size += (params * 3);
      total -= params;
    }
    if (total > 9) {
      int params = total - 9;
      size += (params * 2);
      total -= params;
    }
    if (total > 0) {
      size += total;
    }
    return size;
  }
}
