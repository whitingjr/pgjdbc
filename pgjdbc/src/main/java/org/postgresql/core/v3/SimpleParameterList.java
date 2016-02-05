/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
* Copyright (c) 2004, Open Cloud Limited.
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.core.v3;

import org.postgresql.core.Oid;
import org.postgresql.core.PGStream;
import org.postgresql.core.ParameterList;
import org.postgresql.core.Utils;
import org.postgresql.util.ByteConverter;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.util.StreamWrapper;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


/**
 * Parameter list for a single-statement V3 query.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
class SimpleParameterList implements V3ParameterList {

  private final static byte IN = 1;
  private final static byte OUT = 2;
  private final static byte INOUT = IN | OUT;

  private final static byte TEXT = 0;
  private final static byte BINARY = 4;

  SimpleParameterList(int paramCount, ProtocolConnectionImpl protoConnection) {
    this.paramValues = new ArrayList<Object>(paramCount);
    this.paramTypes = new ArrayList<Integer>(paramCount);
    this.flags = new ArrayList<Byte>(paramCount);
    this.encoded = new ArrayList<byte[]>(paramCount);
    fill(paramCount);
    this.protoConnection = protoConnection;
  }

  SimpleParameterList(List<Object> values, List<Integer> types, List<Byte>
      flags, List<byte[]> encoded, ProtocolConnectionImpl protoConnection) {
    this.paramValues = new ArrayList<Object>(values.size());
    this.paramValues.addAll(values);
    this.paramTypes = new ArrayList<Integer>(types.size());
    this.paramTypes.addAll(types);
    this.flags = new ArrayList<Byte>(flags.size());
    this.flags.addAll(flags);
    this.encoded = new ArrayList<byte[]>(encoded.size());
    this.encoded.addAll(encoded);
    this.protoConnection = protoConnection;
  }

  public void registerOutParameter(int index, int sqlType) throws SQLException {
    fill(index);

    byte b = flags.get(index - 1);
    flags.set(index - 1, b |= OUT);
  }

  private void bind(int index, Object value, int oid, byte binary) throws SQLException {
    fill(index);

    --index;

    encoded.set(index, null);
    paramValues.set(index, value);
    flags.set(index, (byte) (direction(index) | IN | binary));

    // If we are setting something to an UNSPECIFIED NULL, don't overwrite
    // our existing type for it. We don't need the correct type info to
    // send this value, and we don't want to overwrite and require a
    // reparse.
    if (oid == Oid.UNSPECIFIED && paramTypes.get(index) != Oid.UNSPECIFIED && value == NULL_OBJECT) {
      return;
    }

    paramTypes.set(index, oid);
  }

  public int getParameterCount() {
    return paramValues.size();
  }

  public int getOutParameterCount() {
    int count = 0;
    int size = paramTypes.size();
    for (int i = 0; i < size; i++) {
      if ((direction(i) & OUT) == OUT) {
        count++;
      }
    }
    // Every function has at least one output.
    if (count == 0) {
      count = 1;
    }
    return count;

  }

  public int getInParameterCount() {
    int count = 0;
    int size = paramTypes.size();
    for (int i = 0; i < size; i++) {
      if (direction(i) != OUT) {
        count++;
      }
    }
    return count;
  }

  public void setIntParameter(int index, int value) throws SQLException {
    byte[] data = new byte[4];
    ByteConverter.int4(data, 0, value);
    bind(index, data, Oid.INT4, BINARY);
  }

  public void setLiteralParameter(int index, String value, int oid) throws SQLException {
    bind(index, value, oid, TEXT);
  }

  public void setStringParameter(int index, String value, int oid) throws SQLException {
    bind(index, value, oid, TEXT);
  }

  public void setBinaryParameter(int index, byte[] value, int oid) throws SQLException {
    bind(index, value, oid, BINARY);
  }

  public void setBytea(int index, byte[] data, int offset, int length) throws SQLException {
    bind(index, new StreamWrapper(data, offset, length), Oid.BYTEA, BINARY);
  }

  public void setBytea(int index, InputStream stream, int length) throws SQLException {
    bind(index, new StreamWrapper(stream, length), Oid.BYTEA, BINARY);
  }

  public void setBytea(int index, InputStream stream) throws SQLException {
    bind(index, new StreamWrapper(stream), Oid.BYTEA, BINARY);
  }

  public void setNull(int index, int oid) throws SQLException {

    byte binaryTransfer = TEXT;

    if (protoConnection.useBinaryForReceive(oid)) {
      binaryTransfer = BINARY;
    }
    bind(index, NULL_OBJECT, oid, binaryTransfer);
  }

  public String toString(int index) {
    if (paramValues.size() < index) {
      fill(index);
    }
    --index;
    if (paramValues.get(index) == null) {
      return "?";
    } else if (paramValues.get(index) == NULL_OBJECT) {
      return "NULL";
    } else if ((flags.get(index) & BINARY) == BINARY) {
      // handle some of the numeric types

      switch (paramTypes.get(index)) {
        case Oid.INT2:
          short s = ByteConverter.int2((byte[]) paramValues.get(index), 0);
          return Short.toString(s);

        case Oid.INT4:
          int i = ByteConverter.int4((byte[]) paramValues.get(index), 0);
          return Integer.toString(i);

        case Oid.INT8:
          long l = ByteConverter.int8((byte[]) paramValues.get(index), 0);
          return Long.toString(l);

        case Oid.FLOAT4:
          float f = ByteConverter.float4((byte[]) paramValues.get(index), 0);
          return Float.toString(f);

        case Oid.FLOAT8:
          double d = ByteConverter.float8((byte[]) paramValues.get(index), 0);
          return Double.toString(d);
      }
      return "?";
    } else {
      String param = paramValues.get(index).toString();
      boolean hasBackslash = param.indexOf('\\') != -1;

      // add room for quotes + potential escaping.
      StringBuilder p = new StringBuilder(3 + param.length() * 11 / 10);

      boolean standardConformingStrings = false;
      boolean supportsEStringSyntax = false;
      if (protoConnection != null) {
        standardConformingStrings = protoConnection.getStandardConformingStrings();
        supportsEStringSyntax = protoConnection.getServerVersionNum() >= 80100;
      }

      if (hasBackslash && !standardConformingStrings && supportsEStringSyntax) {
        p.append('E');
      }

      p.append('\'');
      try {
        p = Utils.escapeLiteral(p, param, standardConformingStrings);
      } catch (SQLException sqle) {
        // This should only happen if we have an embedded null
        // and there's not much we can do if we do hit one.
        //
        // The goal of toString isn't to be sent to the server,
        // so we aren't 100% accurate (see StreamWrapper), put
        // the unescaped version of the data.
        //
        p.append(param);
      }
      p.append('\'');
      return p.toString();
    }
  }

  public void checkAllParametersSet() throws SQLException {
    int size = paramTypes.size();
    for (int i = 0; i < size; ++i) {
      if (direction(i) != OUT && paramValues.get(i) == null) {
        throw new PSQLException(GT.tr("No value specified for parameter {0}.", i + 1),
            PSQLState.INVALID_PARAMETER_VALUE);
      }
    }
  }

  public void convertFunctionOutParameters() {
    int size = paramTypes.size();
    for (int i = 0; i < size; ++i) {
      if (direction(i) == OUT) {
        paramTypes.set(i, Oid.VOID);
        paramValues.set(i, "null");
      }
    }
  }

  //
  // bytea helper
  //

  private static void streamBytea(PGStream pgStream, StreamWrapper wrapper) throws IOException {
    byte[] rawData = wrapper.getBytes();
    if (rawData != null) {
      pgStream.Send(rawData, wrapper.getOffset(), wrapper.getLength());
      return;
    }

    pgStream.SendStream(wrapper.getStream(), wrapper.getLength());
  }

  public List<Integer> getTypeOIDs() {
    return paramTypes;
  }

  //
  // Package-private V3 accessors
  //

  int getTypeOID(int index) {
    return paramTypes.get(index - 1);
  }

  boolean hasUnresolvedTypes() {
    for (int paramType : paramTypes) {
      if (paramType == Oid.UNSPECIFIED) {
        return true;
      }
    }
    return false;
  }

  void setResolvedType(int index, int oid) {
    // only allow overwriting an unknown value
    if (paramTypes.get(index - 1) == Oid.UNSPECIFIED) {
      paramTypes.set(index - 1, oid);
    } else if (paramTypes.get(index - 1) != oid) {
      throw new IllegalArgumentException("Can't change resolved type for param: " + index + " from "
          + paramTypes.get(index - 1) + " to " + oid);
    }
  }

  boolean isNull(int index) {
    return (paramValues.get(index - 1) == NULL_OBJECT);
  }

  boolean isBinary(int index) {
    return (flags.get(index - 1) & BINARY) != 0;
  }

  private byte direction(int index) {
    return (byte) (flags.get(index) & INOUT);
  }

  int getV3Length(int index) {
    --index;

    // Null?
    if (paramValues.get(index) == NULL_OBJECT) {
      throw new IllegalArgumentException("can't getV3Length() on a null parameter");
    }

    // Directly encoded?
    if (paramValues.get(index) instanceof byte[]) {
      return ((byte[]) paramValues.get(index) ).length;
    }

    // Binary-format bytea?
    if (paramValues.get(index) instanceof StreamWrapper) {
      return ((StreamWrapper) paramValues.get(index)).getLength();
    }

    // Already encoded?
    if (encoded.get(index) == null) {
      // Encode value and compute actual length using UTF-8.
      encoded.set(index, Utils.encodeUTF8(paramValues.get(index).toString()));
    }

    return encoded.get(index).length;
  }

  void writeV3Value(int index, PGStream pgStream) throws IOException {
    --index;

    // Null?
    if (paramValues.get(index) == NULL_OBJECT) {
      throw new IllegalArgumentException("can't writeV3Value() on a null parameter");
    }

    // Directly encoded?
    if (paramValues.get(index) instanceof byte[]) {
      pgStream.Send((byte[]) paramValues.get(index));
      return;
    }

    // Binary-format bytea?
    if (paramValues.get(index) instanceof StreamWrapper) {
      streamBytea(pgStream, (StreamWrapper) paramValues.get(index));
      return;
    }

    // Encoded string.
    if (encoded.get(index) == null) {
      encoded.set(index, Utils.encodeUTF8((String) paramValues.get(index)));
    }
    pgStream.Send(encoded.get(index));
  }

  public ParameterList copy() {
    return new SimpleParameterList(paramValues, paramTypes, flags, encoded, protoConnection);
  }

  public void clear() {
    int size = paramValues.size();
    /* See BatchExecuteTest, the existing parameters are kept.*/
    paramValues.clear();
    paramTypes.clear();
    encoded.clear();
    flags.clear();
    fill(size);
  }

  public SimpleParameterList[] getSubparams() {
    return null;
  }

  public List<Object> getValues() {
    return paramValues;
  }

  @Override
  public List<Integer> getParamTypes() {
    return paramTypes;
  }

  @Override
  public List<Byte> getFlags() {
    return flags;
  }

  @Override
  public List<byte[]> getEncoding() {
    return encoded;
  }

  @Override
  public void replace(ParameterList list) {
    if (list instanceof org.postgresql.core.v3.SimpleParameterList ) {
      /* only v3.SimpleParameterList is compatible with this type
      we need to create copies of our parameters, otherwise the values can be changed */
      clear();
      SimpleParameterList spl = (SimpleParameterList) list;
      paramValues.addAll(spl.getValues());
      paramTypes.addAll(spl.getParamTypes());
      flags.addAll(spl.getFlags());
      encoded.addAll(spl.getEncoding());
    }
  }

  @Override
  public void addAll(ParameterList list) {
    if (list instanceof org.postgresql.core.v3.SimpleParameterList ) {
      /* only v3.SimpleParameterList is compatible with this type
      Backing collections have been sized based on parameter count. */
      SimpleParameterList spl = (SimpleParameterList) list;
      paramValues.addAll(spl.getValues());
      paramTypes.addAll(spl.getParamTypes());
      flags.addAll(spl.getFlags());
      encoded.addAll(spl.getEncoding());
    }
  }

  /** Add elements to collections to preserve behaviour of array use. Avoids
   * NPE or ArrayIndexOutOfBoundsException.
   *
   * @param size number of parameters
   */
  public void fill(int size) {
    if (size <= paramTypes.size()) { // shrink
      return;
    }
    int n = size - paramTypes.size();
    for (int i = 0; i < n; i += 1) {
      paramValues.add(null);
      paramTypes.add(Oid.UNSPECIFIED);
      flags.add((byte)0);
      encoded.add(null);
    }
  }

  @Override
  public void shrink(int size) {
    int n = paramValues.size() - size;
    for (int i = 0; i < n ; i += 1) {
      paramValues.remove(paramValues.size() - 1);
    }
    n = paramTypes.size() - size;
    for (int i = 0; i < n; i += 1 ) {
      paramTypes.remove(paramTypes.size() - 1);
    }
    n = flags.size() - size;
    for (int i = 0; i < n; i += 1) {
      flags.remove(flags.size() - 1);
    }
    n = encoded.size() - size;
    for (int i = 0; i < n ; i += 1) {
      encoded.remove(encoded.size() - 1);
    }
  }

  private final List<Object> paramValues;
  private final List<Integer> paramTypes;
  private final List<Byte> flags;
  private final List<byte[]> encoded;
  private final ProtocolConnectionImpl protoConnection;

  /**
   * Marker object representing NULL; this distinguishes "parameter never set" from "parameter set
   * to null".
   */
  private final static Object NULL_OBJECT = new Object();
}

