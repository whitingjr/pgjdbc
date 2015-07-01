package org.postgresql.core.v3;

import org.postgresql.core.ParameterList;
import org.postgresql.core.Query;

/**
 * Purpose of this object is to support batched query re write 
 * behaviour. Responsibility for tracking the batch size and implement the clean up
 * of the query fragments after the batch execute is complete.
 * All other operations are passed back to the SimpleQuery implementation.
 * This object extends rather than composed because type QueryExecutorImpl.sendQuery 
 * relies on SimpleQuery.
 * @author Jeremy Whiting
 *
 */
public class BatchedQueryDecorator extends SimpleQuery {

    private SimpleQuery query = null;
    private final String[] originalFragments ;
    private int batchedCount = 0;
    
    public BatchedQueryDecorator(Query q) {
        super(new String[0], null); // protoConn is encapsulated. making a constructor call to SQ with object references difficult.
        originalFragments = new String[q.getFragments().length] ;
        System.arraycopy(q.getFragments(), 0, originalFragments, 0, q.getFragments().length);
        batchedCount = q.getBatchSize();
        if (q instanceof SimpleQuery) {
            query = (SimpleQuery)q;
        }
    }
    
    public void reset() {
        query.clearFragments();
        query.addQueryFragments(originalFragments);
        batchedCount = 0;
        query.resetBatchedCount();
    }
    
    @Override
    public String[] getFragments() {
        return query.getFragments();
    }
    
    @Override
    public int getBatchSize() {
        return batchedCount;
    }
    
    public void addQueryFragments( String[] additional ) {
        String[] existing = query.getFragments();
        String[] replacement = new String[existing.length + additional.length];
        System.arraycopy(existing, 0, replacement, 0, existing.length);
        System.arraycopy(additional, 0, replacement, existing.length, additional.length);
        query.clearFragments();
        query.addQueryFragments(replacement);
    }
    
    @Override
    public boolean isStatementDescribed() {
        return query.isStatementDescribed();
    }
    
    @Override
    public boolean isEmpty() {
        return query.isEmpty();
    }
    
    @Override
    public ParameterList createParameterList() {
        return query.createParameterList();
    }
    
    @Override
    public void incrementBatchSize() {
        batchedCount += 1;
    }
    
    @Override
    public boolean isStatementReWritableInsert() {
        return query.isStatementReWritableInsert();
    }
    
    @Override
    public void close() {
        query.close();
    }
    
    @Override
    public String toString(ParameterList parameters) {
        return query.toString();
    }
    @Override
    public void clearFragments() {
        query.clearFragments();
    }
    
    @Override
    public SimpleQuery[] getSubqueries() {
        return query.getSubqueries();
    }
}
