package org.postgresql.core.v3;

import static org.postgresql.core.Oid.UNSPECIFIED;

import java.lang.ref.PhantomReference;
import java.util.Arrays;

import org.postgresql.core.Field;
import org.postgresql.core.ParameterList;
import org.postgresql.core.Query;
import org.postgresql.core.Utils;

/**
 * Purpose of this object is to support batched query re write 
 * behaviour. Responsibility for tracking the batch size and implement the clean up
 * of the query fragments after the batch execute is complete.
 * Intended to be used to wrap a Query that is present in the batchStatements
 * collection. Or wrap the preparedQuery to add parsed status tracking for
 * individual Statements varying by parameter count.  
 * The methods re-direct calls to the composed SimpleQuery instance.
 * 
 * @author Jeremy Whiting jwhiting@redhat.com
 *
 */
public class BatchedQueryDecorator extends SimpleQuery {

    private SimpleQuery query;
    private final String[] originalFragments ;
    private final int[] originalPreparedTypes;
    private boolean isPreparedTypesSet;
    private final Field[] originalFields;
    private boolean isFieldsSet;
    private int batchedQueryCount = 1;
    private Integer isParsed = 0;

    private String batchedStatementName;
    private byte[] batchedEncodedName;
    
    /**
     * Set up the decorator with data structures that are sized correctly for a batch with a 
     * single row. Sizing for meta data needs to be the same. Meta data may not be available
     * yet.
     * @param q
     */
    public BatchedQueryDecorator(Query q) {
        super(null, null); // protoConn is encapsulated. making a constructor call to SQ with object references difficult.
        if (q instanceof SimpleQuery) {
            query = (SimpleQuery)q;
        
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
                Arrays.fill(originalPreparedTypes, UNSPECIFIED); // use -1 to indicate an unset primitive
            }

            originalFields = new Field[paramCount];
            if (query.getFields() != null && query.getFields().length > 0) {
                System.arraycopy(query.getFields(), 0, originalFields, 0, paramCount);
            }
            query.setStatementName(null);
        } else { // unsupported type of Query 
            originalFragments = new String[0];
            originalPreparedTypes = new int[0];
            originalFields = new Field[0];
        }
    }
    
    public void reset() {
        batchedQueryCount = 1;
        
        int[] initializedTypes = null;
        for (int pos = 0; pos < originalPreparedTypes.length; pos += 1) {
            if (originalPreparedTypes[pos]==UNSPECIFIED) {
                initializedTypes = new int[0];
                break;
            }
        }
        if (initializedTypes==null) {
            initializedTypes = originalPreparedTypes;
        }
        Field[] initializedFields = null;
        for (int pos = 0; pos<=originalFields.length; pos += 1) {
            if (originalFields[pos]==null ) {
                initializedFields = new Field[0];
                break;
            }
        }
        if (initializedFields==null) {
            initializedFields = originalFields;
        }
        query.reset(originalFragments, initializedTypes, initializedFields);
        query.resetBatchedCount();
        setStatementName(null);
        assert query.getStatementTypes().length == initializedTypes.length;
    }
    
    @Override
    public int getBatchSize() {
        return batchedQueryCount;
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
        batchedQueryCount += 1;
    }
    
    /**
     * Check if this unique batched query has been described.
     */
    @Override
    public boolean isStatementDescribed() {
        return query.isStatementDescribed();
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
     * The original meta data may need updating.
     */
    @Override
    public void setStatementTypes(int[] types) {
        query.setStatementTypes(types);
        if (isOriginalStale(types)) {
            updateOriginal(types);
        }
    }
    
    @Override
    public int[] getStatementTypes() {
        int types[] = query.getStatementTypes();
        if (isOriginalStale(types)) {
            /* Use opportunity to update originals if a ParameterDescribe has 
             * been called. */
            updateOriginal(types);
        }
        return resizeTypes();
    }
    
    /** 
     * Check fields and update if out of sync
     */
    private int[] resizeTypes() {
        // provide types depending on batch size, which may vary
        int expected = (originalFragments.length-1)*getBatchSize();
        int[] types = query.getStatementTypes();
        
        if (types == null) {
            types = fill(expected);
        }
        if (types.length < expected){
            types = fill(expected);
        }
        query.setStatementTypes(types);
        return types;
    }
    
    private int[] fill(int expected){
        int[] types = Arrays.copyOf(originalPreparedTypes, expected);
        for (int row = 1; row < getBatchSize(); row += 1) {
            System.arraycopy(originalPreparedTypes, 0, types, 
                row*originalPreparedTypes.length, originalPreparedTypes.length);
        }
        return types;
    }
    
    @Override
    boolean isPreparedFor(int[] paramTypes) {
        resizeTypes();
        return query.isPreparedFor(paramTypes) && isStatementParsed()  ;
    }
    
    @Override
    boolean hasUnresolvedTypes() {
        return query.hasUnresolvedTypes();
    }
    
    @Override
    byte[] getEncodedStatementName() {
        if (batchedEncodedName==null) {
            String n = getStatementName();
            if (n != null) {
                batchedEncodedName = Utils.encodeUTF8(n);
            }
        }
        return batchedEncodedName;
    }
    
    /**
     * The original meta data may need updating.
     */
    @Override
    public void setFields(Field[] fields) {
        query.setFields(fields);
        // changed during Describe or reWrite.
        if (isOriginalStale(fields)) {
            updateOriginal(fields);
        }
    }
    
    /**
     * The driver does depend on
     * null rather than empty collection.
     * @return known fields or null
     */
    @Override
    public Field[] getFields() {
        // changed during Describe
        Field[] current = query.getFields();
        if (isOriginalStale(current)) {
            updateOriginal(current);
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
        if (statementName == null) {
            batchedStatementName = null;
            batchedEncodedName = null;
        } else {
            query.setStatementName(statementName);
        }
    }
    
    /**
     * Detect when the vanilla Field meta data is out of date. It
     * indicates since construction a ParameterDescription message has been 
     * processed and the original needs replacing with detailed type information.
     * @param fields
     * @return
     */
    private boolean isOriginalStale(Field[] fields) {
        if (isFieldsSet) {
            return false;
        }
        if (fields == null) {
            return false;
        }
        if (fields.length == 0) {
            return false;
        }
        if (fields.length < originalFields.length) {
            return false;
        }
        int maxPos = originalFields.length - 1;
        for (int pos = 0; pos <= maxPos; pos += 1) {
            if (originalFields[pos]==null && fields[pos] != null) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Detect when the vanilla prepared type meta data is out of date.
     * @param preparedTypes meta data to compare with
     * @return true if the constructed meta data is out of date
     */
    private boolean isOriginalStale(int[] preparedTypes) {
        if (isPreparedTypesSet) {
            return false;
        }
        if (preparedTypes == null) {
            return false;
        }
        if (preparedTypes.length == 0) {
            return false;
        }
        if (preparedTypes.length < originalPreparedTypes.length) {
            return false;
        }
        int maxPos = originalPreparedTypes.length - 1;
        for (int pos = 0; pos <= maxPos ; pos += 1  ) { 
            if ( originalPreparedTypes[pos]== UNSPECIFIED && preparedTypes[pos] != UNSPECIFIED) {
                return true;
            }
        }
        return false;
    }
    
    private void updateOriginal(Field[] fields) {
        if (fields==null) {
            return;
        }
        if (fields.length==0) {
            return;
        }
        isFieldsSet = true;
        int maxPos = originalFields.length -1;
        for (int pos = 0; pos <= maxPos; pos+=1) {
            if (fields[pos] != null ) {
                if (originalFields[pos]==null) {
                    originalFields[pos] = fields[pos];
                }
            } else {
                isFieldsSet = false;
            }
        }            
    }
    
    private void updateOriginal(int[] preparedTypes) {
        if (preparedTypes==null) {
            return;
        }
        if (preparedTypes.length==0) {
            return;
        }
        isPreparedTypesSet = true;
        int maxPos = originalPreparedTypes.length -1;
        for (int pos = 0; pos <= maxPos; pos++) {
            if (preparedTypes[pos]!=UNSPECIFIED ) {
                if ( originalPreparedTypes[pos]==UNSPECIFIED ){
                    originalPreparedTypes[pos]=preparedTypes[pos];
                }
            } else {
                isPreparedTypesSet = false;
            }
        }
    }
    
    private int getCurrentParameterCount() {
        return (getFragments().length -1) ;
    }
    
    private boolean isStatementParsed() {
        return isParsed.equals(getCurrentParameterCount());
    }
    
    /**
     * Method to receive notification of the parsed/prepared status of the
     * statement.
     * @param prepared state
     */
    public void registerQueryParsedStatus(boolean prepared) {
        isParsed = getCurrentParameterCount();
    }
}
