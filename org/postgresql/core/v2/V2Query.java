/*-------------------------------------------------------------------------
*
* Copyright (c) 2003-2014, PostgreSQL Global Development Group
* Copyright (c) 2004, Open Cloud Limited.
*
*
*-------------------------------------------------------------------------
*/
package org.postgresql.core.v2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.postgresql.core.*;

/**
 * Query implementation for all queries via the V2 protocol.
 */
class V2Query implements Query {
    V2Query(String query, boolean withParameters, ProtocolConnection pconn) {

        useEStringSyntax = pconn.getServerVersionNum() >= 80100;
        boolean stdStrings = pconn.getStandardConformingStrings();

        if (!withParameters)
        {
            fragments = new String[] { query };
            return ;
        }

        // Parse query and find parameter placeholders.

        List v = new ArrayList();
        int lastParmEnd = 0;

        char []aChars = query.toCharArray();

        for (int i = 0; i < aChars.length; ++i)
        {
            switch (aChars[i])
            {
            case '\'': // single-quotes
                i = Parser.parseSingleQuotes(aChars, i, stdStrings);
                break;

            case '"': // double-quotes
                i = Parser.parseDoubleQuotes(aChars, i);
                break;

            case '-': // possibly -- style comment
                i = Parser.parseLineComment(aChars, i);
                break;

            case '/': // possibly /* */ style comment
                i = Parser.parseBlockComment(aChars, i);
                break;
            
            case '$': // possibly dollar quote start
                i = Parser.parseDollarQuotes(aChars, i);
                break;

            case '?':
                if (i+1 < aChars.length && aChars[i+1] == '?') /* let '??' pass */
                    i = i+1;
                else
                {
                    v.add(query.substring (lastParmEnd, i));
                    lastParmEnd = i + 1;
                }
                break;

            default:
                break;
            }
        }

        v.add(query.substring (lastParmEnd, query.length()));

        fragments = new String[v.size()];
        for (int i = 0 ; i < fragments.length; ++i)
            fragments[i] = Parser.unmarkDoubleQuestion((String)v.get(i), stdStrings);
    }

    public ParameterList createParameterList() {
        if (fragments.length == 1)
            return NO_PARAMETERS;

        return new SimpleParameterList(fragments.length - 1, useEStringSyntax);
    }

    public String toString(ParameterList parameters) {
        StringBuilder sbuf = new StringBuilder(fragments[0]);
        for (int i = 1; i < fragments.length; ++i)
        {
            if (parameters == null)
                sbuf.append("?");
            else
                sbuf.append(parameters.toString(i));
            sbuf.append(fragments[i]);
        }
        return sbuf.toString();
    }

    public void close() {
    }

    public String[] getFragments() {
        return fragments;
    }

    public boolean isStatementDescribed() {
        return false;
    }

    public boolean isEmpty()
    {
        return fragments.length == 1 && "".equals(fragments[0]);
    }
    
    @Override
    public void addQueryFragments(String[] additional) {
        String[] replacement = Arrays.copyOf(this.fragments, this.fragments.length + additional.length);
        int pos = this.fragments.length;
        for (int i = 0; i < replacement.length; i += 1) {
            replacement[pos++] = additional[i];
        }
        this.fragments = replacement;
    }

    @Override
    public boolean isStatementReWritableInsert() {
        return this.statementReWritableInsert;
    }
    
    public void setStatementReWritableInsert(boolean canReWrite) {
        this.statementReWritableInsert = canReWrite;
    }
    
    @Override
    public void incrementBatchSize() {
        this.batchSize += 1;
    }
    
    @Override
    public int getBatchSize() {
        return this.batchSize;
    }

    private static final ParameterList NO_PARAMETERS = new SimpleParameterList(0, false);

    private String[] fragments;      // Query fragments, length == # of parameters + 1
    
    private final boolean useEStringSyntax; // whether escaped string syntax should be used
    
    private boolean statementReWritableInsert;
    
    private int batchSize = 0;
}

