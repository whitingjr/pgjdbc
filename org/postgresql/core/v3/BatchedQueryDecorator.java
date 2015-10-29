package org.postgresql.core.v3;

import java.lang.ref.PhantomReference;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.postgresql.core.Field;
import org.postgresql.core.ParameterList;
import org.postgresql.core.Query;
import org.postgresql.core.Utils;

import static org.postgresql.core.Oid.UNSPECIFIED;

/**
 * Purpose of this object is to support batched query re write 
 * behaviour. Responsibility for tracking the batch size and implement the clean up
 * of the query fragments after the batch execute is complete.
 * Intended to be used to wrap a Query that is present in the batchStatements
 * collection.
 * The methods re-direct calls instead to the composed SimpleQuery instance. Rather
 * than the inherited methods.
 * 
 * @author Jeremy Whiting
 *
 */
public class BatchedQueryDecorator extends SimpleQuery {

    private SimpleQuery query;
    private final String[] originalFragments ;
    private final int[] originalPreparedTypes;
    private boolean isPreparedTypesSet;
    private final Field[] originalFields;
    private boolean isFieldsSet;
    private int batchedCount = 0;
    private Map<Integer,Object> isDescribed = new HashMap<Integer,Object>(51);
    private static final String NAME_FORMAT = "%1$s_P_%2$d";
    /** statementName is isolated from the query field to allow the prepared 
     * statement uniqueness to be tracked/detected. same for the encodedName field.
     */
    private String statementName;
    private byte[] encodedName;
    private String originalParentName;
    
    /**
     * Set up the decorator with data structures that are sized correctly for a batch with a 
     * single row. Sizing for meta data needs to be the same. Meta data may not be available
     * yet. Type information should be available after a ParameterDescription message is processed.
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
        } else { // unsupported type of Query 
            originalFragments = new String[0];
            originalPreparedTypes = new int[0];
            originalFields = new Field[0];
        }
        batchedCount = q.getBatchSize();
    }
    
    public void reset() {
        batchedCount = 0;
        isDescribed.put(getFragments().length-1, null);
        
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
    
    /**
     * Check if this unique batched query has been described.
     */
    @Override
    public boolean isStatementDescribed() {
        return isDescribed.containsKey(getFragments().length-1);
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
        String parentName = query.getStatementName();
        if (this.statementName==null && parentName != null) {
            if (originalParentName==null) {
                originalParentName=parentName;
            }
            this.statementName=String.format(NAME_FORMAT, originalParentName, getFragments().length-1);
        }
        return this.statementName;
    }
    
    /**
     * Detect if a Describe has been processed. The original meta data may need
     * updating.
     */
    @Override
    public void setStatementTypes(int[] paramTypes) {
        query.setStatementTypes(paramTypes);
        if (isOriginalStale(paramTypes)) {
            updateOriginal(paramTypes);
        }
    }
    
    @Override
    public int[] getStatementTypes() {
        int types[] = query.getStatementTypes();
        if (isOriginalStale(types)) {
            updateOriginal(types);
        }
        return types;
    }
    
    @Override
    boolean isPreparedFor(int[] paramTypes) {
        return query.isPreparedFor(paramTypes) && isDescribed.containsKey( getFragments().length -1 );
    }
    
    @Override
    boolean hasUnresolvedTypes() {
        return query.hasUnresolvedTypes();
    }
    
    @Override
    byte[] getEncodedStatementName() {
        if (this.encodedName==null) {
            this.encodedName = Utils.encodeUTF8(getStatementName());
        }
        return this.encodedName;
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
            updateOriginal(fields);
        }
    }
    
    @Override
    public Field[] getFields() {
        // changed during Describe or reWrite.
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
        if (statementDescribed) {
            isDescribed.put(getFragments().length-1, null);
        } else {
            isDescribed.remove(getFragments().length-1);
        }
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
        if (originalParentName==null) {
            originalParentName = statementName;
        }
    }
    
    /**
     * Detect when the vanilla Field meta data is out of date. It
     * indicates since construction a ParameterDescription message has been 
     * processed. This object needs updating.
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
     * Detect when the vanilla prepared type meta data is out of date. It
     * indicates since construction a ParameterDescription message has been 
     * processed. This object needs updating.
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
        int maxPos = originalFields.length -1;
        for (int pos = 0; pos <= maxPos; pos+=1) {
            if (originalFields[pos]==null && fields[pos] != null) {
                originalFields[pos] = fields[pos];
            }
            if (pos==maxPos) {
                isFieldsSet=true;
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
        int maxPos = originalPreparedTypes.length -1;
        for (int pos = 0; pos <= maxPos; pos++) {
            if (originalPreparedTypes[pos]==UNSPECIFIED && preparedTypes[pos]!=UNSPECIFIED) {
                originalPreparedTypes[pos]=preparedTypes[pos];
            }
            if (pos==maxPos) {
                isPreparedTypesSet=true;
            }
        }
    }
}
