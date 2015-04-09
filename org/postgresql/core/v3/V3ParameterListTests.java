package org.postgresql.core.v3;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import org.postgresql.core.Logger;
import org.postgresql.core.PGStream;
import org.postgresql.util.HostSpec;

import junit.framework.TestCase;

/**
 * Test cases to make sure the parameterlist implementation works 
 * as expected. Test case located in this package as the object is
 * package private.
 * @author whitingjr
 *
 */
public class V3ParameterListTests extends TestCase{

    private ProtocolConnectionImpl proto = null;
    
    public void setUp() {
        try {
            HostSpec hostSpec = new HostSpec("blah", 5432);
            PGStream stream = new PGStream(hostSpec);
            this.proto = new ProtocolConnectionImpl (stream, "", "", new Properties(), new Logger(), 5000);
        } catch (IOException ioe) {
            
        }
    }
    
    public void testMergeOfParameterLists() {
        try {
            SimpleParameterList l1 = new SimpleParameterList(8, proto);
            Integer one = new Integer(1);
            Integer two = new Integer(2);
            Integer three = new Integer(3);
            Integer four = new Integer(4);
            l1.setIntParameter(1, one);
            l1.setIntParameter(2, two);
            l1.setIntParameter(3, three);
            l1.setIntParameter(4, four);
            SimpleParameterList l2 = new SimpleParameterList(4, proto);
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
            assertNotNull(vals[1]);
            assertNotNull(vals[2]);
            assertNotNull(vals[3]);
            assertNotNull(vals[4]);
            assertNotNull(vals[5]);
            assertNotNull(vals[6]);
            assertNotNull(vals[7]);
        } catch (SQLException sqle) {
            fail(sqle.getMessage());
        } 
    }
    
    public void testAddAll() {
        try {
            SimpleParameterList l1 = new SimpleParameterList(4, proto);
            Integer one = new Integer(1);
            Integer two = new Integer(2);
            Integer three = new Integer(3);
            Integer four = new Integer(4);
            l1.setIntParameter(1, one);
            l1.setIntParameter(2, two);
            l1.setIntParameter(3, three);
            l1.setIntParameter(4, four);
            
            assertNotNull(l1.getValues());
            Object[] vals = l1.getValues();
            assertNotNull(vals[0]);
            assertNotNull(vals[1]);
            assertNotNull(vals[2]);
            assertNotNull(vals[3]);
            assertFalse(0 == l1.getV3Length(1)); //size position
            assertFalse(0 == l1.getV3Length(2));
            assertFalse(0 == l1.getV3Length(3));
            assertFalse(0 == l1.getV3Length(4));
            assertNotNull(l1.getEncoding());
            assertNotNull(l1.getFlags());
            assertNotNull(l1.getFlags()[0]);
            assertNotNull(l1.getFlags()[1]);
            assertNotNull(l1.getFlags()[2]);
            assertNotNull(l1.getFlags()[3]);
            assertNotNull(l1.getParamTypes());
            assertNotNull(l1.getParamTypes()[0]);
            assertNotNull(l1.getParamTypes()[1]);
            assertNotNull(l1.getParamTypes()[2]);
            assertNotNull(l1.getParamTypes()[3]);
            
            SimpleParameterList l2 = new SimpleParameterList(8, proto);
            l2.addAll(l1);
            assertNotNull(l2.getValues());
            assertEquals(8, l2.getValues().length);
            assertNotNull(l2.getValues()[0]);
            assertNotNull(l2.getValues()[1]);
            assertNotNull(l2.getValues()[2]);
            assertNotNull(l2.getValues()[3]);
            assertNull(l2.getValues()[4]);
            assertNull(l2.getValues()[5]);
            assertNull(l2.getValues()[6]);
            assertNull(l2.getValues()[7]);
            
        } catch (SQLException sqle) {
            fail(sqle.getMessage());
        }
    }
}
