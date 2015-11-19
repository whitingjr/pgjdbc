package org.postgresql.test.jdbc2;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.TestCase;

import org.postgresql.PGProperty;
import org.postgresql.core.Oid;
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
            assertNotNull(bqd.getFragments());

            pstmt.setInt(1, 5);
            pstmt.setInt(2, 6);
            pstmt.addBatch();//statement three, this should be collapsed into prior statement
            batchedCount = bqd.getBatchSize();

            assertEquals(3, batchedCount);
            int[] types = bqd.getStatementTypes();
            assertEquals(6, types.length);
            assertEquals(Oid.INT4, types[0]);
            assertEquals(Oid.INT4, types[1]);
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
            
            Method mgSN = BatchedQueryDecorator.class.getDeclaredMethod("getStatementName", new Class[]{});
            mgSN.setAccessible(true);
            assertNotNull(mgSN);
            Object obsn = mgSN.invoke(bqd, new Object[]{});
            assertNotNull(obsn);
            assertTrue(obsn instanceof String);
            String bsn = (String)obsn;
            assertEquals("S_2", bsn);
            Method mgESN = BatchedQueryDecorator.class.getDeclaredMethod("getEncodedStatementName", new Class[]{});
            mgESN.setAccessible(true);
            assertNotNull(mgESN);
            Object obesn = mgESN.invoke(bqd, new Object[]{});
            assertNotNull(obesn);
            assertTrue(obsn instanceof String);
            String besn = (String)obesn;
            assertEquals("S_2".getBytes(), besn);
            
            outcome = pstmt.executeBatch();
            assertNotNull(outcome);
            assertEquals(3, outcome.length);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[0]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[1]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[2]);
            
            Field fBSN = BatchedQueryDecorator.class.getDeclaredField("batchedStatementName");
            fBSN.setAccessible(true);
            assertNotNull(fBSN);
            assertEquals(null, fBSN.get(bqd));
            Field fEBSN = BatchedQueryDecorator.class.getDeclaredField("batchedEncodedName");
            fEBSN.setAccessible(true);
            assertNotNull(fEBSN);
            assertEquals(null, fEBSN.get(bqd));
            
            assertEquals(1, bqd.getBatchSize());
            assertNotNull(bqd.getStatementTypes());
            assertEquals(2, bqd.getStatementTypes().length);
            assertNotNull(bqd.getFragments());
            assertEquals(initStmtFragCount, bqd.getFragments().length);
            assertEquals(initParamCount, bqd.getStatementTypes().length);
            assertEquals(initParamCount, resetParamCount);
            con.commit();
            
            pstmt.setInt(1, 1);
            pstmt.setInt(2, 2);
            pstmt.addBatch(); //initial pass _8
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
            
            pstmt.setInt(1, 7);
            pstmt.setInt(2, 8);
            pstmt.addBatch();
            assertEquals(4, bqd.getBatchSize());
            assertEquals(9, bqd.getFragments().length);
            assertEquals(8, bqd.getStatementTypes().length);
            

            outcome = pstmt.executeBatch();
            assertNotNull(outcome);
            assertEquals(4, outcome.length);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[0]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[1]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[2]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[3]);

            pstmt.setInt(1, 1);
            pstmt.setInt(2, 2);
            pstmt.addBatch(); //initial pass _6
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
            
            outcome = pstmt.executeBatch();
            assertNotNull(outcome);
            assertEquals(3, outcome.length);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[0]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[1]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[2]);
            
            pstmt.setInt(1, 1);
            pstmt.setInt(2, 2);
            pstmt.addBatch(); //initial pass _6
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
            
            outcome = pstmt.executeBatch();
            assertNotNull(outcome);
            assertEquals(3, outcome.length);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[0]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[1]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[2]);
            
            pstmt.setInt(1, 1);
            pstmt.setInt(2, 2);
            pstmt.addBatch(); //initial pass _6
            assertEquals(1, bqd.getBatchSize());
            pstmt.setInt(1, 3);
            pstmt.setInt(2, 4);
            pstmt.addBatch();
            assertEquals(2, bqd.getBatchSize());
            assertEquals(5, bqd.getFragments().length);
            assertEquals(4, bqd.getStatementTypes().length);
            
            outcome = pstmt.executeBatch();
            assertNotNull(outcome);
            assertEquals(2, outcome.length);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[0]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[1]);
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
    
    /** 
     * 
     */
    public void testUnspecifiedParameterType()
        throws SQLException{
        PreparedStatement pstmt = null;
        try {
            /*
             * The connection is configured so the batch rewrite optimization
             * is enabled. See setUp()
             */
            pstmt = con.prepareStatement("INSERT INTO testunspecified VALUES (?,?)");
            
            pstmt.setInt(1, 1);
            pstmt.setDate(2, new Date(1970, 01, 01));
            pstmt.addBatch();
            
            pstmt.setInt(1, 2);
            pstmt.setDate(2, new Date(1971, 01, 01));
            pstmt.addBatch();
            
            assertTrue(pstmt instanceof AbstractJdbc2Statement);
            AbstractJdbc2Statement s = (AbstractJdbc2Statement)pstmt;
            Field f = AbstractJdbc2Statement.class.getDeclaredField("preparedQuery");
            assertNotNull(f);
            f.setAccessible(true);
            Object fObject = f.get(s);
            assertNotNull(fObject);
            assertTrue(fObject instanceof BatchedQueryDecorator);
            BatchedQueryDecorator bqd = (BatchedQueryDecorator)fObject;
            int[] types = bqd.getStatementTypes();
            assertNotNull(types);
            assertEquals(Oid.INT4, types[0]);
            assertEquals(Oid.UNSPECIFIED, types[1]);
            assertEquals(Oid.INT4, types[2]);
            assertEquals(Oid.UNSPECIFIED, types[3]);
            
            int[] outcome = pstmt.executeBatch();
            assertNotNull(outcome);
            assertEquals(2, outcome.length);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[0]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[1]);
            
            pstmt.setInt(1, 1);
            pstmt.setDate(2, new Date(1970, 01, 01));
            pstmt.addBatch();
            pstmt.setInt(1, 2);
            pstmt.setDate(2, new Date(1971, 01, 01));
            pstmt.addBatch();
            
            types = bqd.getStatementTypes();
            assertEquals(Oid.INT4, types[0]);
            assertFalse(Oid.UNSPECIFIED == types[1]);
            assertEquals(Oid.INT4, types[2]);
            assertFalse(Oid.UNSPECIFIED == types[3]);
            outcome = pstmt.executeBatch();
            
            assertNotNull(outcome);
            assertEquals(2, outcome.length);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[0]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[1]);
            
        } catch (SQLException sqle) {
            fail ("Failed to execute batch. Reason:" +sqle.getMessage());
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
    
    /**
     * Test to check the statement can provide the necessary number of 
     * prepared type fields. This is after running with a batch size of 1.   
     * @throws SQLException
     */
    public void testVaryingTypeCounts()
        throws SQLException{
        PreparedStatement pstmt = null;
        try {
            pstmt = con.prepareStatement("INSERT INTO testunspecified VALUES (?,?)");
            pstmt.setInt(1, 1);
            pstmt.setDate(2, new Date(1970, 01, 01));
            pstmt.addBatch();
            
            int[] outcome = pstmt.executeBatch();
            assertNotNull(outcome);
            assertEquals(1, outcome.length);
            assertEquals(1, outcome[0]);
            
            pstmt.setInt(1, 1);
            pstmt.setDate(2, new Date(1970, 01, 01));
            pstmt.addBatch();
            pstmt.setInt(1, 2);
            pstmt.setDate(2, new Date(1971, 01, 01));
            pstmt.addBatch();
            
            pstmt.setInt(1, 3);
            pstmt.setDate(2, new Date(1972, 01, 01));
            pstmt.addBatch();
            pstmt.setInt(1, 4);
            pstmt.setDate(2, new Date(1973, 01, 01));
            pstmt.addBatch();
            
            outcome = pstmt.executeBatch();
            assertNotNull(outcome);
            assertEquals(4, outcome.length);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[0]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[1]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[2]);
            assertEquals(Statement.SUCCESS_NO_INFO, outcome[3]);
            
        } catch (SQLException sqle) {
            fail ("Failed to execute statements added to a batch with varying batch counts. Reason:" +sqle.getMessage());
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
        TestUtil.createTable(con, "testunspecified", "pk INTEGER, bday TIMESTAMP");

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
        TestUtil.dropTable(con, "testunspecified");
        TestUtil.closeDB(con);
    }

    private Connection con;
}
