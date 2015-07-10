package org.postgresql.core.v3;

import java.lang.ref.PhantomReference;

import org.postgresql.core.Field;
import org.postgresql.core.ParameterList;
import org.postgresql.core.Query;

/**
 * Purpose of this object is to support batched query re write 
 * behaviour. Responsibility for tracking the batch size and implement the clean up
 * of the query fragments after the batch execute is complete.
 * The methods re-direct calls instead to the composed SimpleQuery instance. Rather
 * than the inherited methods.
 * 
 * @author Jeremy Whiting
 *
 */
public class BatchedQueryDecorator extends SimpleQuery {

    private SimpleQuery query = null;
    private final String[] originalFragments ;
    private int batchedCount = 0;
    
    public BatchedQueryDecorator(Query q) {
        super(null, null); // protoConn is encapsulated. making a constructor call to SQ with object references difficult.
        if (q instanceof SimpleQuery) {
            query = (SimpleQuery)q;
        }
        originalFragments = new String[q.getFragments().length] ;
        System.arraycopy(q.getFragments(), 0, originalFragments, 0, q.getFragments().length);
        batchedCount = q.getBatchSize();
        
    }
    
    public void reset() {
        query.clearFragments();
        query.addQueryFragments(originalFragments);
        batchedCount = 0;
        query.resetBatchedCount();
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
    public String[] getFragments() {
        return this.query.getFragments();
    }
    
    @Override
    public void incrementBatchSize() {
        batchedCount += 1;
    }
    
    @Override
    public boolean isStatementDescribed() {
        return this.query.isStatementDescribed();
    }
    
    @Override
    public ParameterList createParameterList() {
            return this.query.createParameterList();
    }
    
    @Override
    public boolean isEmpty() {
        return this.query.isEmpty();
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

    @Override
    public int getMaxResultRowSize() {
        return query.getMaxResultRowSize();
    }
    
    @Override
    String getStatementName() {
        return query.getStatementName();
    }
    
    @Override
    void setStatementTypes(int[] paramTypes) {
        query.setStatementTypes(paramTypes);
    }
    
    @Override
    boolean isPreparedFor(int[] paramTypes) {
        return query.isPreparedFor(paramTypes);
    }
    
    @Override
    boolean hasUnresolvedTypes() {
        return query.hasUnresolvedTypes();
    }
    
    @Override
    byte[] getEncodedStatementName() {
        return query.getEncodedStatementName();
    }
    
    @Override
    void setFields(Field[] fields) {
        query.setFields(fields);
    }
    
    @Override
    boolean isPortalDescribed() {
        return query.isPortalDescribed();
    }
    
    @Override
    void setPortalDescribed(boolean portalDescribed) {
        query.setPortalDescribed(portalDescribed);
    }
    
    @Override
    void setStatementDescribed(boolean statementDescribed) {
        query.setStatementDescribed(statementDescribed);
    }
    
    @Override
    void setCleanupRef(PhantomReference cleanupRef) {
        query.setCleanupRef(cleanupRef);
    }
    
    @Override
    public void setStatementReWritableInsert(boolean isReWriteable) {
        query.setStatementReWritableInsert(isReWriteable);
    }
    
    @Override
    void setStatementName(String statementName) {
        query.setStatementName(statementName);
    }
}
