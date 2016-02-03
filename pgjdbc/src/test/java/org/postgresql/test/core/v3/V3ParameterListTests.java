/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2003-2016, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */

package org.postgresql.test.core.v3;

import org.postgresql.core.Logger;
import org.postgresql.core.PGStream;
import org.postgresql.core.ParameterList;
import org.postgresql.core.v2.SocketFactoryFactory;
import org.postgresql.test.TestUtil;
import org.postgresql.util.ByteConverter;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;

import junit.framework.TestCase;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.net.SocketFactory;

/**
 * Test cases to make sure the parameterlist implementation works as expected.
 * Test case located in different package as the package private implementation.
 * Reflection used to crack open and access the class. Not pretty but works.
 *
 * @author Jeremy Whiting jwhiting@redhat.com
 *
 */
public class V3ParameterListTests extends TestCase {

  public void testAddAll() {
    try {
      SocketFactory socketFactory = SocketFactoryFactory.getSocketFactory(System.getProperties());
      HostSpec hostSpec = new HostSpec(TestUtil.getServer(), 5432);
      PGStream stream = new PGStream(socketFactory, hostSpec);
      ClassLoader cl = this.getClass().getClassLoader();
      Class clsProtoConnImpl = Class.forName(
          "org.postgresql.core.v3.ProtocolConnectionImpl", true, cl);
      Constructor c = clsProtoConnImpl.getDeclaredConstructor(PGStream.class,
          String.class, String.class, Properties.class, Logger.class,
          Integer.TYPE);
      c.setAccessible(true);
      Object oPCI = c.newInstance(new Object[] { stream, "", "",
          new Properties(), new Logger(), 5000 });
      c = null;
      assertNotNull(oPCI);

      Class clsSPL = Class.forName(
          "org.postgresql.core.v3.SimpleParameterList", true, cl);
      Constructor initSPL = clsSPL.getDeclaredConstructor(new Class[] {
          Integer.TYPE, clsProtoConnImpl });
      initSPL.setAccessible(true);
      Object s1SPL = initSPL.newInstance(4, oPCI);
      assertNotNull(s1SPL);
      Method msetIP = clsSPL.getMethod("setIntParameter", Integer.TYPE,
          Integer.TYPE);
      assertNotNull(msetIP);
      msetIP.setAccessible(true);
      msetIP.invoke(s1SPL, 1, 1);
      msetIP.invoke(s1SPL, 2, 2);
      msetIP.invoke(s1SPL, 3, 3);
      msetIP.invoke(s1SPL, 4, 4);

      Object s2SPL = initSPL.newInstance(new Object[] { 4, oPCI });
      assertNotNull(s2SPL);
      msetIP.invoke(s2SPL, 1, 5);
      msetIP.invoke(s2SPL, 2, 6);
      msetIP.invoke(s2SPL, 3, 7);
      msetIP.invoke(s2SPL, 4, 8);

      Method mappAll = clsSPL.getMethod("addAll", ParameterList.class);
      mappAll.setAccessible(true);
      mappAll.invoke(s1SPL, s2SPL);

      Method mgetValues = clsSPL.getMethod("getValues");
      mgetValues.setAccessible(true);
      Object oValues = mgetValues.invoke(s1SPL);
      assertNotNull(oValues);
      assertTrue(oValues instanceof List<?>);
      List<Object> listValues = (List<Object>) oValues;
      assertEquals(8, listValues.size());
      assertNotNull(listValues.get(0));
      assertNotNull(listValues.get(1));
      assertNotNull(listValues.get(2));
      assertNotNull(listValues.get(3));
      assertNotNull(listValues.get(4));
      assertNotNull(listValues.get(5));
      assertNotNull(listValues.get(6));
      assertNotNull(listValues.get(7));
      assertTrue(listValues.get(0) instanceof byte[]);
      assertTrue(listValues.get(1) instanceof byte[]);
      assertTrue(listValues.get(2) instanceof byte[]);
      assertTrue(listValues.get(3) instanceof byte[]);
      assertTrue(listValues.get(4) instanceof byte[]);
      assertTrue(listValues.get(5) instanceof byte[]);
      assertTrue(listValues.get(6) instanceof byte[]);
      assertTrue(listValues.get(7) instanceof byte[]);
      byte[] b = new byte[4];
      ByteConverter.int4(b, 0, 1);
      assertTrue(Arrays.equals(b, (byte[]) listValues.get(0)));
      ByteConverter.int4(b, 0, 2);
      assertTrue(Arrays.equals(b, (byte[]) listValues.get(1)));
      ByteConverter.int4(b, 0, 3);
      assertTrue(Arrays.equals(b, (byte[]) listValues.get(2)));
      ByteConverter.int4(b, 0, 4);
      assertTrue(Arrays.equals(b, (byte[]) listValues.get(3)));
      ByteConverter.int4(b, 0, 5);
      assertTrue(Arrays.equals(b, (byte[]) listValues.get(4)));
      ByteConverter.int4(b, 0, 6);
      assertTrue(Arrays.equals(b, (byte[]) listValues.get(5)));
      ByteConverter.int4(b, 0, 7);
      assertTrue(Arrays.equals(b, (byte[]) listValues.get(6)));
      ByteConverter.int4(b, 0, 8);
      assertTrue(Arrays.equals(b, (byte[]) listValues.get(7)));
    } catch (ClassNotFoundException cnfe) {
      fail(cnfe.getMessage());
    } catch (NoSuchMethodException nsme) {
      fail(nsme.getMessage());
    } catch (InvocationTargetException ite) {
      fail(ite.getMessage());
    } catch (IllegalAccessException iae) {
      fail(iae.getMessage());
    } catch (InstantiationException ie) {
      fail(ie.getMessage());
    } catch (IOException ioe) {
      fail(ioe.getMessage());
    } catch (PSQLException psqle ) {
      fail(psqle.getMessage());
    }
  }

  public void testBackingCollections() {
    try {
      SocketFactory socketFactory = SocketFactoryFactory.getSocketFactory(System.getProperties());
      HostSpec hostSpec = new HostSpec(TestUtil.getServer(), 5432);
      PGStream stream = new PGStream(socketFactory, hostSpec);
      ClassLoader cl = this.getClass().getClassLoader();
      Class clsProtoConnImpl = Class.forName(
          "org.postgresql.core.v3.ProtocolConnectionImpl", true, cl);
      Constructor c = clsProtoConnImpl.getDeclaredConstructor(PGStream.class,
          String.class, String.class, Properties.class, Logger.class,
          Integer.TYPE);
      c.setAccessible(true);
      Object oPCI = c.newInstance(new Object[] { stream, "", "",
          new Properties(), new Logger(), 5000 });
      assertNotNull(oPCI);

      Class clsSPL = Class.forName(
          "org.postgresql.core.v3.SimpleParameterList", true, cl);
      Constructor initSPL = clsSPL.getDeclaredConstructor(new Class[] {
          Integer.TYPE, clsProtoConnImpl });
      initSPL.setAccessible(true);
      Object s1SPL = initSPL.newInstance(new Object[] { 4, oPCI });
      assertNotNull(s1SPL);
      Method msetIP = clsSPL.getMethod("setIntParameter", Integer.TYPE,
          Integer.TYPE);
      assertNotNull(msetIP);
      msetIP.setAccessible(true);
      msetIP.invoke(s1SPL, 1, 1);
      msetIP.invoke(s1SPL, 2, 2);
      msetIP.invoke(s1SPL, 3, 3);
      msetIP.invoke(s1SPL, 4, 4);

      Method mgetValues = clsSPL.getMethod("getValues");
      mgetValues.setAccessible(true);
      Object oValues = mgetValues.invoke(s1SPL);
      assertNotNull(oValues);
      assertTrue(oValues instanceof List<?>);
      List<Object> listValues = (List<Object>) oValues;
      assertEquals(4, listValues.size());

      Method mgetV3Length = clsSPL.getDeclaredMethod("getV3Length",
          Integer.TYPE);
      mgetV3Length.setAccessible(true);
      assertEquals(4, mgetV3Length.invoke(s1SPL, 1));
      assertEquals(4, mgetV3Length.invoke(s1SPL, 2));
      assertEquals(4, mgetV3Length.invoke(s1SPL, 3));
      assertEquals(4, mgetV3Length.invoke(s1SPL, 4));

      Method mgetEncoding = clsSPL.getMethod("getEncoding");
      mgetEncoding.setAccessible(true);
      assertNotNull(mgetEncoding.invoke(s1SPL));

      Method mgetFlags = clsSPL.getMethod("getFlags");
      mgetFlags.setAccessible(true);
      Object oFlags = mgetFlags.invoke(s1SPL);
      assertTrue(oFlags instanceof List<?>);
      List<Byte> flags = (List<Byte>) oFlags;
      assertEquals(4, flags.size());
      assertNotNull(flags.get(0));
      assertNotNull(flags.get(1));
      assertNotNull(flags.get(2));
      assertNotNull(flags.get(3));

      Method mgetParamTypes = clsSPL.getMethod("getParamTypes");
      mgetParamTypes.setAccessible(true);
      Object oparamTypes = mgetParamTypes.invoke(s1SPL);
      assertTrue(oparamTypes instanceof List<?>);
      List<Integer> paramTypes = (List<Integer>) oparamTypes;
      assertEquals(4, paramTypes.size());
      assertNotNull(paramTypes.get(0));
      assertNotNull(paramTypes.get(1));
      assertNotNull(paramTypes.get(2));
      assertNotNull(paramTypes.get(3));
    } catch (ClassNotFoundException cnfe) {
      fail(cnfe.getMessage());
    } catch (NoSuchMethodException nsme) {
      fail(nsme.getMessage());
    } catch (InvocationTargetException ite) {
      fail(ite.getMessage());
    } catch (IllegalAccessException iae) {
      fail(iae.getMessage());
    } catch (InstantiationException ie) {
      fail(ie.getMessage());
    } catch (IOException ioe) {
      fail(ioe.getMessage());
    } catch (PSQLException psqle ) {
      fail(psqle.getMessage());
    }
  }
}
