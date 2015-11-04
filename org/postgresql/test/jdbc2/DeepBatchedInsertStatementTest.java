package org.postgresql.test.jdbc2;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.TestCase;

import org.postgresql.PGProperty;
import org.postgresql.core.Query;
import org.postgresql.core.v3.BatchedQueryDecorator;
import org.postgresql.jdbc2.AbstractJdbc2Statement;
import org.postgresql.test.TestUtil;

/**
 * This object tests the internals of the BatchedStatementDecorator during
 * execution. Rather than rely on testing at the jdbc api layer which
 * BatchedInsertReWriteEnabledTest and BatchedInsertStatementPreparingTest
 * rely on. 
 *
 */
public class DeepBatchedInsertStatementTest extends TestCase
{
    public void testDeepInternalsBatchedQueryDecorator()
        throws SQLException
    {
        PreparedStatement pstmt = null;
        try {
            /*
             * The connection is configured so the batch rewrite optimization
             * is enabled. See setUp()
             */
            pstmt = con.prepareStatement("INSERT INTO testbatch VALUES (?,?)");
            int initParamCount = 2;
            int initStmtFragCount = 3;
            assertTrue(pstmt instanceof AbstractJdbc2Statement);
            AbstractJdbc2Statement s = (AbstractJdbc2Statement)pstmt;
            
            // hackers delight
            Field f = AbstractJdbc2Statement.class.getDeclaredField("preparedQuery");
            assertNotNull(f);
            f.setAccessible(true);
            
            pstmt.setInt(1, 1);
            pstmt.setInt(2, 2);
            pstmt.addBatch(); //initial pass
            Object fObject = f.get(s);
            assertNotNull(fObject);
            assertTrue(fObject instanceof Query);
            assertFalse(fObject instanceof BatchedQueryDecorator);
            
            pstmt.setInt(1, 3);
            pstmt.setInt(2, 4);
            pstmt.addBatch();//preparedQuery will be wrapped

            fObject = f.get(s);
            assertNotNull(fObject);
            assertTrue(fObject instanceof Query);
            assertTrue(fObject instanceof BatchedQueryDecorator);
            BatchedQueryDecorator bqd = (BatchedQueryDecorator)fObject;
            int batchedCount = bqd.getBatchSize(); 
            assertEquals(2, batchedCount);
            assertTrue(bqd.getStatementTypes() == null);
            assertNotNull(bqd.getFragments());

            pstmt.setInt(1, 5);
            pstmt.setInt(2, 6);
            pstmt.addBatch();//statement three, this should be collapsed into prior statement
            batchedCount = bqd.getBatchSize();
            assertEquals(3, batchedCount);
            
            int[] outcome = pstmt.executeBatch();
            
            assertNotNull(outcome);
            assertEquals(3, outcome.length);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[0]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[1]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[2]);
            
            /* The statement will have been reset. */
            Method m = BatchedQueryDecorator.class.getDeclaredMethod("getCurrentParameterCount", new Class[]{});
            m.setAccessible(true);
            assertNotNull(m);
            Object mRetObj = m.invoke(bqd, new Object[]{});
            assertNotNull(mRetObj);
            assertTrue(mRetObj instanceof Integer);
            int resetParamCount = ((Integer)mRetObj).intValue();
            
            batchedCount = bqd.getBatchSize();
            assertEquals(1, batchedCount);
            assertNotNull(bqd.getStatementTypes());
            assertEquals(2, bqd.getStatementTypes().length);
            assertNotNull(bqd.getFragments());
            assertEquals(initStmtFragCount, bqd.getFragments().length);
            assertEquals(initParamCount, bqd.getStatementTypes().length);
            assertEquals(initParamCount, resetParamCount);
            
            pstmt.setInt(1, 1);
            pstmt.setInt(2, 2);
            pstmt.addBatch(); //initial pass
            
            fObject = f.get(s);
            assertNotNull(fObject);
            assertTrue(fObject instanceof BatchedQueryDecorator);
            assertEquals(1, bqd.getBatchSize());
            
            pstmt.setInt(1, 3);
            pstmt.setInt(2, 4);
            pstmt.addBatch();
            assertEquals(2, bqd.getBatchSize());
            assertEquals(5, bqd.getFragments().length);
            assertEquals(4, bqd.getStatementTypes().length);
            
            pstmt.setInt(1, 5);
            pstmt.setInt(2, 6);
            pstmt.addBatch();
            assertEquals(3, bqd.getBatchSize());
            assertEquals(7, bqd.getFragments().length);
            assertEquals(6, bqd.getStatementTypes().length);

            
            
        } catch (SQLException sqle) {
            fail ("Failed to execute three statements added to a batch. Reason:" +sqle.getMessage());
        } catch (Exception e) {
            if (e.getCause() == null) {
                fail (String.format("Exception thrown:[%1$s]", e.getMessage()));
            } else {
                fail (String.format("Exception thrown:[%1$s] cause [%2$s]", e.getMessage(),e.getCause().getMessage()));
            }
        } finally {
            if (null != pstmt) {pstmt.close();}
            con.rollback();
        }
    }
    
    public DeepBatchedInsertStatementTest(String name) {
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
        props.setProperty(PGProperty.PREPARE_THRESHOLD .getName(), "1");
        
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

    private Connection con;
    
}
