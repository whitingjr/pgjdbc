package org.postgresql.test.jdbc2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.TestCase;

import org.postgresql.PGProperty;
import org.postgresql.test.TestUtil;

public class BatchedInsertReWriteEnabled extends TestCase{
    
    private Connection con;
    
    /**
     * Check batching using two individual statements that are both the same type.
     * Test to check the re-write optimization behaviour.
     * @throws SQLException
     */
    public void testBatchWithReWrittenRepeatedInsertStatementOptimizationEnabled() throws SQLException {
        PreparedStatement pstmt = null;
        try {
            /*
             * The connection is configured so the batch rewrite optimization
             * is enabled. See setUp()
             */
            pstmt = con.prepareStatement("INSERT INTO testbatch VALUES (?,?)");
            pstmt.setInt(1, 1);
            pstmt.setInt(2, 2);
            pstmt.addBatch(); //statement one
            pstmt.setInt(1, 3);
            pstmt.setInt(2, 4);
            pstmt.addBatch();//statement two, this should be collapsed into prior statement
            pstmt.setInt(1, 5);
            pstmt.setInt(2, 6);
            pstmt.addBatch();//statement three, this should be collapsed into prior statement
            int[] outcome = pstmt.executeBatch();

            assertNotNull(outcome);
            assertEquals(3, outcome.length);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[0]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[1]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[2]);
        } catch (SQLException sqle) {
            fail ("Failed to execute three statements added to a batch. Reason:" +sqle.getMessage());
        }
        
        /* Now check the ps can be reused. The batched statement should be
         * reset and have no knowledge of prior re-written batch.
         */
        try {
            pstmt.setInt(1, 1);
            pstmt.setInt(2, 2);
            pstmt.addBatch();
            pstmt.setInt(1, 3);
            pstmt.setInt(2, 4);
            pstmt.addBatch();
            pstmt.setInt(1, 5);
            pstmt.setInt(2, 6);
            pstmt.addBatch();
            pstmt.setInt(1, 7);
            pstmt.setInt(2, 8);
            pstmt.addBatch();
            int[] outcome = pstmt.executeBatch();

            assertNotNull(outcome);
            assertEquals(4, outcome.length);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[0]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[1]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[2]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[3]);
        } catch (SQLException sqle) {
            fail ("Failed to execute four statements added to a re used Prepared Statement. Reason:" +sqle.getMessage());
        } finally {
            if (null != pstmt) {pstmt.close();}
            con.rollback();
        }
    }
    
    public void testBatchWithReWrittenBatchStatementWithSemiColon() throws SQLException {
        PreparedStatement pstmt = null;
        try {
            /*
             * The connection is configured so the batch rewrite optimization
             * is enabled. See setUp()
             */
            pstmt = con.prepareStatement("INSERT INTO testbatch VALUES (?,?);");
            pstmt.setInt(1, 1);
            pstmt.setInt(2, 2);
            pstmt.addBatch(); //statement one
            pstmt.setInt(1, 3);
            pstmt.setInt(2, 4);
            pstmt.addBatch();//statement two, this should be collapsed into prior statement
            pstmt.setInt(1, 5);
            pstmt.setInt(2, 6);
            pstmt.addBatch();//statement three, this should be collapsed into prior statement
            int[] outcome = pstmt.executeBatch();

            assertNotNull(outcome);
            assertEquals(3, outcome.length);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[0]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[1]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[2]);
        } catch (SQLException sqle) {
            fail ("Failed to execute three statements added to a batch. Reason:" +sqle.getMessage());
        } finally {
            if (null != pstmt) {pstmt.close();}
            con.rollback();
        }
    }
    
    public BatchedInsertReWriteEnabled(String name)
    {
        super(name);
        try
        {
            Class.forName("org.postgresql.Driver");
        }
        catch( Exception ex){}
    }

    // Set up the fixture for this testcase: a connection to a database with
    // a table for this test.
    protected void setUp() throws Exception
    {
        Properties props = new Properties();
        props.setProperty(PGProperty.REWRITE_BATCHED_INSERTS.getName(), Boolean.TRUE.toString());
        
        con = TestUtil.openDB(props);
        Statement stmt = con.createStatement();

        // Drop the test table if it already exists for some reason. It is
        // not an error if it doesn't exist.
        TestUtil.createTable(con, "testbatch", "pk INTEGER, col1 INTEGER");

        stmt.executeUpdate("INSERT INTO testbatch VALUES (1, 0)");
        stmt.close();

        TestUtil.createTable(con, "prep", "a integer, b integer");

        // Generally recommended with batch updates. By default we run all
        // tests in this test case with autoCommit disabled.
        con.setAutoCommit(false);
    }

    // Tear down the fixture for this test case.
    protected void tearDown() throws Exception
    {
        con.setAutoCommit(true);

        TestUtil.dropTable(con, "testbatch");
        TestUtil.closeDB(con);
    }

}
