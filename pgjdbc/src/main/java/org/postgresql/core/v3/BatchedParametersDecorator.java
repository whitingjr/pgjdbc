/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2016, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.core.v3;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.postgresql.core.ParameterList;

/**
 * Read only facarde on an array of SimpleParameterList objects.
 * @author Jeremy Whiting jwhiting@redhat.com
 *
 */
public class BatchedParametersDecorator implements ParameterList{

   private List<ParameterList> l;
   private int[] toid;
   private Object[] tovals;
   public BatchedParametersDecorator(List<ParameterList> pl) {
      l = pl;
   }
   public void registerOutParameter(int index, int sqlType) throws SQLException {
   }
   public int getParameterCount() {
      return 0;
   }
   public int getInParameterCount() {
      int c = 0;
      for (ParameterList list : l) {
         c += list.getInParameterCount();
      }
      return c;
   }
   public int getOutParameterCount() {
      int c = 0;
      for (ParameterList list: l ) {
         c += list.getOutParameterCount();
      }
      return c;
   }
   public int[] getTypeOIDs() {
      if (null == toid) {
         toid = new int[l.get(0).getTypeOIDs().length * l.size()];
         int c = 0;
         for (ParameterList list : l) {
            int[] sub = list.getTypeOIDs();
            System.arraycopy(sub, 0, toid, c, sub.length);
            c += sub.length;
         }
      }
      return toid;
   }
   public void setIntParameter(int index, int value) throws SQLException {
      // un-supported, these methods are not intended to be used during 
      // statement execution phase.
   }
   public void setLiteralParameter(int index, String value, int oid)
         throws SQLException {
      // un-supported
   }
   public void setStringParameter(int index, String value, int oid)
         throws SQLException {
      // un-supported
   }
   public void setBytea(int index, byte[] data, int offset, int length)
         throws SQLException {
      // setIntParameter
   }
   public void setBytea(int index, InputStream stream, int length)
         throws SQLException {
      // un-supported
   }
   public void setBytea(int index, InputStream stream) throws SQLException {
      // un-supported
   }
   public void setBinaryParameter(int index, byte[] value, int oid)
         throws SQLException {
      // un-supported
   }
   public void setNull(int index, int oid) throws SQLException {
      // un-supported
   }
   public ParameterList copy() {
      // un-supported
      return null;
   }
   public void clear() {
      l.clear();
   }
   public String toString(int index) {
      return null;
   }
   public void addAll(ParameterList list) {
      // un-supported
   }
   public void appendAll(ParameterList list) {
   // un-supported
   }
   public Object[] getValues() {
      if (null == tovals) {
         
      }
      return tovals;
   }
}
