/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2011, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/
package org.postgresql.test.jdbc2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.TestCase;

import org.postgresql.test.TestUtil;

/*
 * The purpose of this class is to establish a connection to a postgres database
 * and execute an insert statement on a partition table. The the update count is 
 * checked against the expected value if no exceptions are thrown. The expected
 * count according to the jdbc specification contract is checked. 
 *
 */
public class PartitionedTableTest extends TestCase
{
    private Connection con;
    
    /*
     * Constructor
     */
    public PartitionedTableTest(String name)
    {
        super(name);
    }
    
    /*
     * Setup the database with objects necessary for the test
     */
    protected void setUp() throws Exception
    {
        con = TestUtil.openDB();
        try 
        {
            TestUtil.createTable(con, "ptest", " widget_id int not null ");
            TestUtil.createInheritedTable(con, "ptest_01", "ptest");
            TestUtil.createFunction(con, "ptest_insert_function", " insert into ptest_01 values (NEW.*); return null; ");
            TestUtil.createTrigger(con, "insert_ptest_trigger", "before insert on ptest for each row execute procedure ptest_insert_function();");
        }
        finally
        {
            TestUtil.closeDB(con);  
        }
    }
    
    /*
     * Tear down the test objects.
     * Drop table is necessary to prevent subsequent tests
     * failing creating table.
     */
    protected void tearDown() throws Exception
    {
        con = TestUtil.openDB();
        try
        {
            TestUtil.dropTrigger(con, "insert_ptest_trigger", "ptest");
            TestUtil.dropFunction(con, "ptest_insert_function");
            TestUtil.dropTable(con, "ptest_01");
            TestUtil.dropTable(con, "ptest");
        }
        finally
        {
            TestUtil.closeDB(con);  
        }
    }
    
    /**
     * Perform a test to insert a record into the partitioned table.
     */
    public void testInsertStatement()
        throws Exception
    {
        con = TestUtil.openDB();
        try
        {
            assertNotNull(con);
            Statement stat = con.createStatement();
            assertNotNull(stat);
            assertEquals(1, stat.executeUpdate("insert into ptest values (1)"));
        }
        catch (SQLException sqle)
        {
            fail(sqle.getMessage());
        }
        finally
        {
            TestUtil.closeDB(con);  
        }
    }
}
