package org.postgresql.core.v3;

import java.util.Arrays;

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
        this.originalFragments = new String[q.getFragments().length] ;
        System.arraycopy(q.getFragments(), 0, this.originalFragments, 0, q.getFragments().length);
        this.batchedCount = q.getBatchSize();
        if (q instanceof SimpleQuery) {
            this.query = (SimpleQuery)q;
        }
    }
    
    public void reset() {
        this.query.clearFragments();
        this.query.addQueryFragments(this.originalFragments);
        this.batchedCount = 0;
        this.query.resetBatchedCount();
    }
    
    @Override
    public String[] getFragments() {
        return this.query.getFragments();
    }
    
    @Override
    public int getBatchSize() {
        return this.batchedCount;
    }
    
    public void addQueryFragments( String[] additional ) {
        String[] existing = this.query.getFragments();
        String[] replacement = new String[existing.length + additional.length];
        int pos = 0;
        for (int i = 0; i < existing.length ; i += 1) {
            replacement[i] = existing[i];
            pos += 1;
        }
        for (int i = 0; i < additional.length ; i += 1) {
            replacement[pos] = additional[i];
            pos += 1;
        }
        this.query.clearFragments();
        this.query.addQueryFragments(replacement);
    }
    
    @Override
    public boolean isStatementDescribed() {
        return this.query.isStatementDescribed();
    }
    
    @Override
    public boolean isEmpty() {
        return this.query.isEmpty();
    }
    
    @Override
    public ParameterList createParameterList() {
        return this.query.createParameterList();
    }
    
    @Override
    public void incrementBatchSize() {
        this.batchedCount += 1;
    }
    
    @Override
    public boolean isStatementReWritableInsert() {
        return this.query.isStatementReWritableInsert();
    }
    
    @Override
    public void close() {
        this.query.close();
    }
    
    @Override
    public String toString(ParameterList parameters) {
        return this.query.toString();
    }
    @Override
    public void clearFragments() {
        this.query.clearFragments();
    }
    
    @Override
    public SimpleQuery[] getSubqueries() {
        return this.query.getSubqueries();
    }
}
