package org.postgresql.core.v3;

import java.util.HashMap;
import java.util.Map;

import org.postgresql.core.ParameterList;
import org.postgresql.core.Query;

/**Wrapper for the statement preparedQuery field.
 * Adding behaviour to track the uniqueness of the Statement.
 * 
 *
 */
public class BatchedPreparedStatementDecorator extends SimpleQuery {

    private Query preparedStatement;
    private Map<Integer, Object> isDescribed = new HashMap<Integer, Object>(51);
    
    public BatchedPreparedStatementDecorator(Query q) {
        super(null,null);
        if (q instanceof SimpleQuery) {
            preparedStatement = (SimpleQuery)q;
        }
    }
    
    @Override
    public ParameterList createParameterList() {
        return preparedStatement.createParameterList();
    }

    @Override
    public String toString(ParameterList parameters) {
        return preparedStatement.toString();
    }

    @Override
    public void close() {
        preparedStatement.close();
    }

    @Override
    public boolean isStatementDescribed() {
        return isDescribed.containsKey(getFragments().length-1);
    }

    @Override
    public boolean isEmpty() {
        return preparedStatement.isEmpty();
    }

    @Override
    public boolean isStatementReWritableInsert() {
        return preparedStatement.isStatementReWritableInsert();
    }

    @Override
    public void addQueryFragments(String[] fragments) {
        preparedStatement.addQueryFragments(fragments);
    }

    @Override
    public String[] getFragments() {
        return preparedStatement.getFragments();
    }

    @Override
    public void incrementBatchSize() {
        preparedStatement.incrementBatchSize();
    }

    @Override
    public int getBatchSize() {
        return preparedStatement.getBatchSize();
    }

    @Override
    public void clearFragments() {
        preparedStatement.clearFragments();
    }
    
    @Override
    boolean isPreparedFor(int[] paramTypes) {
        return isPreparedFor(paramTypes) && isDescribed.containsKey(getFragments().length -1);
    }
}
    