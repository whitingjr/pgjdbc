/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2003-2016, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */

package org.postgresql.test.core.v2;

import org.postgresql.core.ParameterList;

import junit.framework.TestCase;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;


/**
 * Test cases to make sure the parameterlist implementation works as expected.
 * Test case located in different package as the package private implementation.
 * Reflection used to crack open and access the class. Not pretty but works.
 *
 * @author Jeremy Whiting jwhiting@redhat.com
 *
 */
public class V2ParameterListTests extends TestCase {

  public void testMergeOfParameterLists() {
    try {
      ClassLoader cl = this.getClass().getClassLoader();
      Class cls = Class.forName("org.postgresql.core.v2.SimpleParameterList",
          true, cl);
      Constructor c = cls.getDeclaredConstructor(Integer.TYPE, Boolean.TYPE);
      c.setAccessible(true);
      Object o1SPL = c
          .newInstance(new Object[] { new Integer(4), Boolean.TRUE });
      assertNotNull(o1SPL);
      Method msetIP = cls.getMethod("setIntParameter", Integer.TYPE,
          Integer.TYPE);
      assertNotNull(msetIP);
      msetIP.setAccessible(true);
      msetIP.invoke(o1SPL, 1, 1);
      msetIP.invoke(o1SPL, 2, 2);
      msetIP.invoke(o1SPL, 3, 3);
      msetIP.invoke(o1SPL, 4, 4);

      Object o2SPL = c
          .newInstance(new Object[] { new Integer(4), Boolean.TRUE });
      msetIP.invoke(o2SPL, 1, 5);
      msetIP.invoke(o2SPL, 2, 6);
      msetIP.invoke(o2SPL, 3, 7);
      msetIP.invoke(o2SPL, 4, 8);
      Method mappendAll = cls.getMethod("addAll", ParameterList.class);
      mappendAll.setAccessible(true);
      assertNotNull(mappendAll);
      mappendAll.invoke(o1SPL, o2SPL);
      Method mgetValues = cls.getMethod("getValues");
      mgetValues.setAccessible(true);

      Object values = mgetValues.invoke(o1SPL);
      assertNotNull(values);
      assertTrue(values instanceof List<?>);
      List<Object> vals = (List<Object>) values;
      assertNotNull(vals.get(0));
      assertEquals("1", vals.get(0));
      assertNotNull(vals.get(1));
      assertEquals("2", vals.get(1));
      assertNotNull(vals.get(2));
      assertEquals("3", vals.get(2));
      assertNotNull(vals.get(3));
      assertEquals("4", vals.get(3));
      assertNotNull(vals.get(4));
      assertEquals("5", vals.get(4));
      assertNotNull(vals.get(5));
      assertEquals("6", vals.get(5));
      assertNotNull(vals.get(6));
      assertEquals("7", vals.get(6));
      assertNotNull(vals.get(7));
      assertEquals("8", vals.get(7));
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
    }
  }
}
