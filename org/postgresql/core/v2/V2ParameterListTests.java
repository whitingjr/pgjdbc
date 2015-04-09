package org.postgresql.core.v2;

import java.sql.SQLException;

import junit.framework.TestCase;

public class V2ParameterListTests extends TestCase{

    public void testMergeOfParameterLists() {
        try {
            SimpleParameterList l1 = new SimpleParameterList(8, true);
            Integer one = new Integer(1);
            Integer two = new Integer(2);
            Integer three = new Integer(3);
            Integer four = new Integer(4);
            l1.setIntParameter(1, one);
            l1.setIntParameter(2, two);
            l1.setIntParameter(3, three);
            l1.setIntParameter(4, four);
            SimpleParameterList l2 = new SimpleParameterList(4, true);
            Integer five = new Integer(5);
            Integer six = new Integer(6);
            Integer seven = new Integer(7);
            Integer eight = new Integer(8);
            l2.setIntParameter(1, five);
            l2.setIntParameter(2, six);
            l2.setIntParameter(3, seven);
            l2.setIntParameter(4, eight);
            l1.appendAll(l2);
            assertNotNull(l1.getValues());
            Object[] vals = l1.getValues();
            assertNotNull(vals[0]);
            assertEquals(one.toString(), (String)vals[0]);
            assertNotNull(vals[1]);
            assertEquals(two.toString(), (String)vals[1]);
            assertNotNull(vals[2]);
            assertEquals(three.toString(), (String)vals[2]);
            assertNotNull(vals[3]);
            assertEquals(four.toString(), (String)vals[3]);
            assertNotNull(vals[4]);
            assertEquals(five.toString(), (String)vals[4]);
            assertNotNull(vals[5]);
            assertEquals(six.toString(), (String)vals[5]);
            assertNotNull(vals[6]);
            assertEquals(seven.toString(), (String)vals[6]);
            assertNotNull(vals[7]);
            assertEquals(eight.toString(), (String)vals[7]);
        } catch (SQLException sqle) {
            fail(sqle.getMessage());
        }
    }
}
