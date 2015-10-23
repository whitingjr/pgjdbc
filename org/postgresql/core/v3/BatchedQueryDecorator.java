package org.postgresql.core.v3;

import java.lang.ref.PhantomReference;
import java.util.Arrays;

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
    public static final int PREPARED_TYPES_UNSET = -1;
    
    /**
     * Set up the decorator with data structures that are sized correctly for a batch with a 
     * single row. Sizing for meta data needs to be the same. Meta data may not be available
     * yet. Available after a Describe message is processed.
     * @param q
     */
    public BatchedQueryDecorator(Query q) {
        super(null, null); // protoConn is encapsulated. making a constructor call to SQ with object references difficult.
        if (q instanceof SimpleQuery) {
            query = (SimpleQuery)q;
        }
        if (query != null) {
            int paramCount = query.getFragments().length - 1;
            
            int fragmentsLength = query.getFragments().length;
            originalFragments = new String[fragmentsLength];
            if (query.getFragments() != null && fragmentsLength > 0) {
                System.arraycopy(query.getFragments(), 0, originalFragments, 0, fragmentsLength);
            }
            
            originalPreparedTypes = new int[paramCount];
            if (query.getStatementTypes() != null && query.getStatementTypes().length > 0) {
                System.arraycopy(query.getStatementTypes(), 0, originalPreparedTypes, 0, paramCount);
            }
            else {
                Arrays.fill(originalPreparedTypes, PREPARED_TYPES_UNSET); // use -1 to indicate an unset primitive
            }

            originalFields = new Field[paramCount];
            if (query.getFields() != null && query.getFields().length > 0) {
                System.arraycopy(query.getFields(), 0, originalFields, 0, paramCount);
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
    
    /**
     * Detect if a Describe has been processed. The original meta data may need
     * updating.
     */
    @Override
    public void setStatementTypes(int[] paramTypes) {
        query.setStatementTypes(paramTypes);
        if (isOriginalStale(paramTypes)) {
            System.arraycopy(paramTypes, 0, originalPreparedTypes, 0, 
                    query.getFragments().length -1);
        }
    }
    
    @Override
    public int[] getStatementTypes() {
        int types[] = query.getStatementTypes();
        if (isOriginalStale(types)) {
            System.arraycopy(types, 0, originalPreparedTypes, 0, 
                    query.getFragments().length -1);
        }
        return types;
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
    
    /**
     * Detect if a Describe has been processed. The original meta data may need
     * updating.
     */
    @Override
    public void setFields(Field[] fields) {
        query.setFields(fields);
        // changed during Describe or reWrite.
        if (isOriginalStale(fields)) {
            System.arraycopy(fields, 0, originalFields, 0,
                    originalFragments.length-1 );
        }
    }
    
    @Override
    public Field[] getFields() {
        // changed during Describe or reWrite.
        Field[] current = query.getFields();
        if (isOriginalStale(current)) {
            System.arraycopy(current, 0, originalFields, 0,
                    originalFragments.length-1 );
        }
        return current;
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
    
    /**
     * Detect when the vanilla Field meta data is out of date. It
     * indicates since construction a describe message has been processed. This
     * object needs updating.
     * @param fields
     * @return
     */
    boolean isOriginalStale(Field[] fields) {
        boolean isOriginalUnSet = true;
        for (Field f: originalFields) {
            isOriginalUnSet |= f==null;
        }
        
        return fields != null && fields.length > 0 && 
                isOriginalUnSet;
    }
    
    /**
     * Detect when the vanilla prepared type meta data is out of date. It
     * indicates since construction a describe message has been processed. This
     * object needs updating.
     * @param preparedTypes meta data to compare with
     * @return true if the constructed meta data is out of date
     */
    boolean isOriginalStale(int[] preparedTypes) {
        boolean isOriginalUnSet = true;
        for (int type: originalPreparedTypes) { 
            isOriginalUnSet |= type == PREPARED_TYPES_UNSET;
        }
        return preparedTypes != null && preparedTypes.length > 0 && 
                isOriginalUnSet;
    }
}
