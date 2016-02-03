/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2015, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.jdbc;

import org.postgresql.core.BaseConnection;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.List;

public class PgParameterMetaData implements ParameterMetaData {

  private final BaseConnection _connection;
  private final List<Integer> _oids;

  public PgParameterMetaData(BaseConnection connection, List<Integer> oids) {
    _connection = connection;
    _oids = oids;
  }

  public String getParameterClassName(int param) throws SQLException {
    checkParamIndex(param);
    return _connection.getTypeInfo().getJavaClass(_oids.get(param - 1));
  }

  public int getParameterCount() {
    return _oids.size();
  }

  /**
   * {@inheritDoc} For now report all parameters as inputs. CallableStatements may have one output,
   * but ignore that for now.
   */
  public int getParameterMode(int param) throws SQLException {
    checkParamIndex(param);
    return ParameterMetaData.parameterModeIn;
  }

  public int getParameterType(int param) throws SQLException {
    checkParamIndex(param);
    return _connection.getTypeInfo().getSQLType(_oids.get(param - 1));
  }

  public String getParameterTypeName(int param) throws SQLException {
    checkParamIndex(param);
    return _connection.getTypeInfo().getPGType(_oids.get(param - 1));
  }

  // we don't know this
  public int getPrecision(int param) throws SQLException {
    checkParamIndex(param);
    return 0;
  }

  // we don't know this
  public int getScale(int param) throws SQLException {
    checkParamIndex(param);
    return 0;
  }

  // we can't tell anything about nullability
  public int isNullable(int param) throws SQLException {
    checkParamIndex(param);
    return ParameterMetaData.parameterNullableUnknown;
  }

  /**
   * {@inheritDoc} PostgreSQL doesn't have unsigned numbers
   */
  public boolean isSigned(int param) throws SQLException {
    checkParamIndex(param);
    return _connection.getTypeInfo().isSigned(_oids.get(param - 1));
  }

  private void checkParamIndex(int param) throws PSQLException {
    if (param < 1 || param > _oids.size()) {
      throw new PSQLException(
          GT.tr("The parameter index is out of range: {0}, number of parameters: {1}.",
              new Object[]{param, _oids.size()}),
          PSQLState.INVALID_PARAMETER_VALUE);
    }
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(getClass());
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }
}
