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
    private final int[] originalPreparedTypes;
    private final Field[] originalFields;
    private int batchedCount = 0;
    
    public BatchedQueryDecorator(Query q) {
        super(null, null); // protoConn is encapsulated. making a constructor call to SQ with object references difficult.
        if (q instanceof SimpleQuery) {
            query = (SimpleQuery)q;
        }
        if (null != query) {
            int preparedParamCount = query.getFragments().length - 1;
            if (null != query.getFragments()) {
                originalFragments = new String[preparedParamCount] ;
                System.arraycopy(query.getFragments(), 0, originalFragments, 0, preparedParamCount);
            } else {
                originalFragments = new String[preparedParamCount];
            }
            //TODO: copy the type information to the query
            if (null != query.getStatementTypes() && query.getStatementTypes() > 0) {
                originalPreparedTypes = new int[query.getFragments().length];
                System.arraycopy(query.getStatementTypes(), 0, originalPreparedTypes, 0, query.getStatementTypes().length);
            } else {
                originalPreparedTypes = new int[0];
            }
            if (null != query.getFields()) {
                originalFields = new Field[query.getFields().length];
                System.arraycopy(query.getFields(), 0, originalFields, 0, query.getFields().length);
            } else {
                originalFields = new Field[0];
            }
        } else { // unsupported type of Query 
            originalFragments = new String[0];
            originalPreparedTypes = new int[0];
            originalFields = new Field[0];
        }
        batchedCount = q.getBatchSize();
    }
    
    public void reset() {
        batchedCount = 0;
        query.reset(originalFragments, originalPreparedTypes, originalFields);
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
    public void setStatementTypes(int[] paramTypes) {
        query.setStatementTypes(paramTypes);
        if (isOriginalStale(paramTypes)) {
            System.arraycopy(paramTypes, 0, originalPreparedTypes, 0, query.getFragments().length -1);
        }
    }
    
    @Override
    public int[] getStatementTypes() {
        return query.getStatementTypes();
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
    public void setFields(Field[] fields) {
        query.setFields(fields);
        // changed during Describe or reWrite.
        if (isOriginalStale(fields)) {
            System.arraycopy(fields, 0, originalFields, 0, originalFragments.length-1 );
        }
    }
    
    @Override
    public Field[] getFields() {
        return query.getFields();
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
    
    boolean isDescribed(int[] preparedTypes) {
        return originalPreparedTypes.length < preparedTypes.length;
    }
    
    boolean isDescribed(Field[] preparedFields) {
        return originalFields.length < preparedFields.length;
    }
    
    boolean isOriginalStale(Field[] fields) {
        return fields != null && fields.length > 0 && originalFields.length==0;
    }
    
    boolean isOriginalStale(int[] preparedTypes) {
        return preparedTypes != null && preparedTypes.length > 0 && originalPreparedTypes.length==0;
    }
}
