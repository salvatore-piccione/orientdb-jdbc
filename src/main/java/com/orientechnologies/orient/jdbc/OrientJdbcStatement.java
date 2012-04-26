/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 * Copyright 2011-2012 TXT e-solutions SpA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.jdbc;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLSelect;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * TODO Add authors
 * 
 * @author Salvatore Piccione (TXT e-solutions SpA - salvatore.piccione AT network.txtgroup.com)
 */
public class OrientJdbcStatement implements Statement {
    
    private static final int MESSAGE_FORMAT_CHOICE_INDEX = 0;
    
    protected static final int NO_UPDATE_RESULT = -1;
    protected static final int NO_LIMIT = 0;
    protected static final int DEFAULT_FETCH_SIZE = NO_LIMIT;
    protected static final int DEFAULT_MAX_FIELD_SIZE = NO_LIMIT;
    protected static final int DEFAULT_MAX_ROWS = NO_LIMIT;
    protected static final int DEFAULT_QUERY_TIMEOUT = NO_LIMIT;
    
    protected static final int DEFAULT_RESULT_SET_TYPE = ResultSet.TYPE_FORWARD_ONLY;
    protected static final int DEFAULT_RESULT_SET_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;
    protected static final int DEFAULT_RESULT_SET_HOLDABILITY = ResultSet.HOLD_CURSORS_OVER_COMMIT;

	protected final OrientJdbcConnection connection;
	protected final ODatabaseDocumentTx database;

	protected OCommandRequest statement;
	protected boolean closed;
	protected OrientJdbcResultSet resultSet;
	protected List<OCommandSQL> batches;
	protected final boolean readOnly;

	protected int resultSetType;
	protected int resultSetConcurrency;
	protected int resultSetHoldability;
	
	protected int preferredResultSetFetchDirection;
	protected int preferredResultSetFetchSize;
	protected int preferredResultSetMaxRows;
	
	protected int updateCount;

	public OrientJdbcStatement(final OrientJdbcConnection iConnection, boolean readOnly) {
		this(iConnection, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY, DEFAULT_RESULT_SET_HOLDABILITY, readOnly);
	}

	/**
	 * 
	 * @param iConnection
	 * @param resultSetType
	 * @param resultSetConcurrency
	 * @throws SQLException
	 */
	public OrientJdbcStatement(OrientJdbcConnection iConnection, int resultSetType, int resultSetConcurrency, boolean readOnly) {
		this(iConnection, resultSetType, resultSetConcurrency, DEFAULT_RESULT_SET_HOLDABILITY, readOnly);
	}

	/**
	 * @param iConnection
	 * @param resultSetType
	 * @param resultSetConcurrency
	 * @param resultSetHoldability
	 */
	public OrientJdbcStatement(OrientJdbcConnection iConnection, int resultSetType, int resultSetConcurrency, int resultSetHoldability, boolean readOnly) {
		this.connection = iConnection;
		this.database = iConnection.getOrientDatabase();
		this.resultSetType = resultSetType;
		this.resultSetConcurrency = resultSetConcurrency;
		this.resultSetHoldability = resultSetHoldability;
		this.readOnly = readOnly;
		
		this.preferredResultSetFetchDirection = OrientJdbcResultSet.DEFAULT_FETCH_DIRECTION;
		this.preferredResultSetMaxRows = DEFAULT_MAX_ROWS;
		this.preferredResultSetFetchSize = OrientJdbcResultSet.DEFAULT_FETCH_SIZE;

        batches = new ArrayList<OCommandSQL>();
		this.updateCount = NO_UPDATE_RESULT;
		this.closed = false;
		this.statement = null;
	}

	@SuppressWarnings("unchecked")
    public boolean execute(final String sql) throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("Statement.executeStatementOnClosedObject",MESSAGE_FORMAT_CHOICE_INDEX, sql));
	    if (readOnly && !sql.startsWith(OCommandExecutorSQLSelect.KEYWORD_SELECT))
	    	throw new SQLException(ErrorMessages.get("Statement.executeUpdateOnReadOnlyConnection", sql));
		if ("".equals(sql))
		    throw new SQLException(ErrorMessages.get("Statement.executeEmptyString"));
	    ODatabaseRecordThreadLocal.INSTANCE.set(database);
		statement = new OCommandSQL(sql);
		
		try {

			Object rawResult = database.command(statement).execute();

			if (rawResult instanceof List<?>) {
				List<ODocument> documents = (List<ODocument>) rawResult;
				
				if (preferredResultSetMaxRows == NO_LIMIT)
                    resultSet = new OrientJdbcResultSet(this, documents, resultSetType, resultSetConcurrency, resultSetHoldability, preferredResultSetFetchDirection, true);
				else if (documents.size() > preferredResultSetMaxRows)
				    resultSet = new OrientJdbcResultSet(this, documents.subList(0, preferredResultSetMaxRows), resultSetType, resultSetConcurrency, resultSetHoldability, preferredResultSetFetchDirection, true);
				else
					resultSet = new OrientJdbcResultSet(this, documents, resultSetType, resultSetConcurrency, resultSetHoldability, preferredResultSetFetchDirection, true);
				
				resultSet.setFetchDirection(preferredResultSetFetchDirection);
								
				updateCount = NO_UPDATE_RESULT;
				return true;
			} else if (rawResult instanceof ODocument) {
			    updateCount = 1;//FIXME NOT SURE THIS IS RIGHT!
			    resultSet = null;
			    return false;
			}
	        else if (rawResult instanceof Integer) {
	            updateCount = (Integer) rawResult;
	            resultSet = null;
	            return false;
	        }
	        else {
	            updateCount = NO_UPDATE_RESULT;
	            resultSet = null;
	            return false;
	        }

		} catch (OQueryParsingException e) {
			throw new SQLSyntaxErrorException(ErrorMessages.get("Statement.syntaxError", sql), e);
		}
	}

	public ResultSet executeQuery(final String sql) throws SQLException {
        if (closed)
            throw new SQLException(ErrorMessages.get("Statement.executeQueryOnClosedObject",MESSAGE_FORMAT_CHOICE_INDEX, sql));
        if ("".equals(sql))
            throw new SQLException(ErrorMessages.get("Statement.executeEmptyString"));
        
        ODatabaseRecordThreadLocal.INSTANCE.set(database);
        
        OSQLSynchQuery<ODocument> queryTMP = new OSQLSynchQuery<ODocument>(sql);
        statement = new OSQLSynchQuery<ODocument>(sql);
		try {
	        List<ODocument> documents = database.query(queryTMP);
			updateCount = NO_UPDATE_RESULT;
			if (preferredResultSetMaxRows == NO_LIMIT)
                resultSet = new OrientJdbcResultSet(this, documents, resultSetType, resultSetConcurrency, resultSetHoldability, preferredResultSetFetchDirection, true);
            else if (documents.size() > preferredResultSetMaxRows)
                resultSet = new OrientJdbcResultSet(this, documents.subList(0, preferredResultSetMaxRows), resultSetType, resultSetConcurrency, resultSetHoldability, preferredResultSetFetchDirection, true);
            else
                resultSet.setFetchDirection(preferredResultSetFetchDirection);
			resultSet.setFetchDirection(preferredResultSetFetchDirection);
			return resultSet;

		} catch (OQueryParsingException e) {
			throw new SQLSyntaxErrorException(ErrorMessages.get("Statement.syntaxError", sql), e);
		}

	}
	
	public int executeUpdate(final String sql) throws SQLException {
        if (closed)
            throw new SQLException(ErrorMessages.get("Statement.executeStatementOnClosedObject", MESSAGE_FORMAT_CHOICE_INDEX, sql));
	    if (readOnly && !sql.startsWith(OCommandExecutorSQLSelect.KEYWORD_SELECT))
	    	throw new SQLException(ErrorMessages.get("Statement.executeUpdateOnReadOnlyConnection", sql));
	    if ("".equals(sql))
            throw new SQLException(ErrorMessages.get("Statement.executeEmptyString"));
        statement = new OCommandSQL(sql);
		executeUpdate();
		return updateCount;
	}
	
	protected void executeUpdate(Object... args) throws SQLException {
        ODatabaseRecordThreadLocal.INSTANCE.set(database);
        resultSet = null;
		try {
	        Object rawResult = database.command(statement).execute(args);

	        if (rawResult instanceof ODocument) {
	            updateCount = 1;//FIXME NOT SURE THIS IS RIGHT
	        }
	        else if (rawResult instanceof Integer) {
	            updateCount = (Integer) rawResult;
	        }
	        else {
	            //throws a SQLException according to the API
	            updateCount = NO_UPDATE_RESULT;
	            throw new SQLException(ErrorMessages.get("Statement.executeQueryAsUpdate", statement.toString()));
	        }
		} catch (OQueryParsingException e) {
		    throw new SQLSyntaxErrorException(ErrorMessages.get("Statement.syntaxError", statement.toString()), e);
		}
	}
	
	protected int executeBatchUpdate(final OCommandSQL sql, Object... args) throws SQLException {
		int result = NO_UPDATE_RESULT;
		try {
	        Object rawResult = database.command(sql).execute(args);

	        if (rawResult instanceof ODocument) {
	            result = 1;//FIXME NOT SURE THIS IS RIGHT
	        }
	        else if (rawResult instanceof Integer) {
	            result = (Integer) rawResult;
	        }
	        else {
	            //throws a SQLException according to the API
	            throw new SQLException(ErrorMessages.get("Statement.executeQueryAsUpdate", sql));
	        }
	        return result;
		} catch (OQueryParsingException e) {
		    throw new SQLSyntaxErrorException(ErrorMessages.get("Statement.syntaxError", sql), e);
		}
	}

	public int executeUpdate(final String sql, int autoGeneratedKeys) throws SQLException {
	    if (Statement.NO_GENERATED_KEYS == autoGeneratedKeys)
	        return this.executeUpdate(sql);
	    else
	        throw new SQLFeatureNotSupportedException();
	}

	public int executeUpdate(final String sql, int[] columnIndexes) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public int executeUpdate(final String sql, String[] columnNames) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public Connection getConnection() throws SQLException {
	    if (closed)
	        throw new SQLException (ErrorMessages.get("Statement.getConnectionFromClosedObject"));
		return connection;
	}

	public void close() throws SQLException {
	    if (!closed) {
    		statement = null;
    		closed = true;
    		updateCount = NO_UPDATE_RESULT;
    		batches = null;
    		resultSet = null;
	    }
	}

	public boolean execute(final String sql, int autoGeneratedKeys) throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("Statement.executeStatementOnClosedObject",MESSAGE_FORMAT_CHOICE_INDEX,sql));
		if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS)
			return execute(sql);
		else if (autoGeneratedKeys == Statement.RETURN_GENERATED_KEYS)
			throw new SQLFeatureNotSupportedException();
		else
			throw new SQLException(ErrorMessages.get("Statement.badGeneratedKeysFlag", autoGeneratedKeys, 
				Statement.NO_GENERATED_KEYS + ", " + Statement.RETURN_GENERATED_KEYS));
	}

	public boolean execute(final String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public boolean execute(final String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void addBatch(final String sql) throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("Statement.addStatementToBatchOnClosedObject"));
	    if (readOnly)
	    	throw new SQLException(ErrorMessages.get("Statement.addStatementToBatchOnReadOnlyConnection"));
	    if (sql.startsWith(OCommandExecutorSQLSelect.KEYWORD_SELECT))
	    	throw new SQLException(ErrorMessages.get("Statement.addQueryToBatch", sql));
	    if ("".equals(sql))
            throw new SQLException(ErrorMessages.get("Statement.addEmptyStringToBatch"));
        batches.add(new OCommandSQL(sql));
	}

	public void cancel() throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void clearBatch() throws SQLException {
	    if (closed)
            throw new SQLException(ErrorMessages.get("Statement.clearBatchOnClosedObject"));
		batches.clear();
	}

	public void clearWarnings() throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public int[] executeBatch() throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("Statement.executeBatchOnClosedObject"));
	    if (readOnly)
	    	throw new SQLException(ErrorMessages.get("Statement.addStatementToBatchOnReadOnlyConnection"));
	    resultSet = null;
	    updateCount = NO_UPDATE_RESULT;
	    
	    ODatabaseRecordThreadLocal.INSTANCE.set(database);
        
		int[] results = new int[batches.size()];
		int i = 0;
		try {
    		for (OCommandSQL sql : batches) {
    			results[i++] = executeBatchUpdate(sql);
    		}
		} catch (Exception e) {
		    throw new BatchUpdateException(e);
		}
		return results;
	}

	public int getFetchDirection() throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("ResultSet.getFetchDirectionFromClosedObject",MESSAGE_FORMAT_CHOICE_INDEX));
		return preferredResultSetFetchDirection;
	}

	public int getFetchSize() throws SQLException {
	    if (closed)
            throw new SQLException(ErrorMessages.get("ResultSet.getFetchSizeFromClosedObject",MESSAGE_FORMAT_CHOICE_INDEX));
	    return preferredResultSetFetchSize;
	}

	public ResultSet getGeneratedKeys() throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public int getMaxFieldSize() throws SQLException {
	    if (closed)
            throw new SQLException(ErrorMessages.get("Statement.getMaxFieldSizeFromClosedObject"));
		return DEFAULT_MAX_FIELD_SIZE;
	}

	public int getMaxRows() throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("Statement.getMaxRowsFromClosedObject"));
	    return preferredResultSetMaxRows;
	}

	public boolean getMoreResults() throws SQLException {
	    //we always return one result!
		return false;
	}

	public boolean getMoreResults(final int current) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public int getQueryTimeout() throws SQLException {
	    if (closed)
            throw new SQLException(ErrorMessages.get("Statement.getQueryTimeoutFromCLosedObject"));
		return DEFAULT_QUERY_TIMEOUT;
	}

	public ResultSet getResultSet() throws SQLException {
	    if (closed) 
	        throw new SQLException(ErrorMessages.get("Statement.getResultSetFromClosedObject"));
	    OrientJdbcResultSet resultSetTmp = resultSet;
	    //set the resultSet to null
	    resultSet = null;
		return resultSetTmp;
	}

	public int getResultSetConcurrency() throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("ResultSet.getConcurrencyFromClosedObject",MESSAGE_FORMAT_CHOICE_INDEX));
	    if (resultSet == null)
	        return DEFAULT_RESULT_SET_CONCURRENCY;
	    else
	        return resultSet.getConcurrency();
	}

	public int getResultSetHoldability() throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("ResultSet.getHoldabilityFromClosedObject",MESSAGE_FORMAT_CHOICE_INDEX));
	    if (resultSet == null)
	        return DEFAULT_RESULT_SET_HOLDABILITY;
	    else
	        return resultSet.getHoldability();
	}

	public int getResultSetType() throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("ResultSet.getTypeFromClosedObject"));
	    if (resultSet == null)
	        return DEFAULT_RESULT_SET_TYPE;
	    else
	        return resultSet.getType();
	}

	public int getUpdateCount() throws SQLException {
		if (closed) throw new SQLException(ErrorMessages.get("Statement.getUpdateCountFromClosedObject",MESSAGE_FORMAT_CHOICE_INDEX));
		return updateCount;
	}

	public SQLWarning getWarnings() throws SQLException {
	   throw new SQLFeatureNotSupportedException();
	}

	public boolean isClosed() throws SQLException {
		return closed;
	}

	public boolean isPoolable() throws SQLException {
	    if (closed) throw new SQLException(ErrorMessages.get("Statement.getPoolableFromClosedObject"));
		return false;
	}

	public void setCursorName(final String name) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void setEscapeProcessing(final boolean enable) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void setFetchDirection(final int direction) throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("ResultSet.setFetchDirectionOnClosedObject", MESSAGE_FORMAT_CHOICE_INDEX));
	    switch (direction) {
	        case ResultSet.FETCH_FORWARD:
	        case ResultSet.FETCH_REVERSE:
                this.preferredResultSetFetchDirection = direction;
                break;
	        case ResultSet.FETCH_UNKNOWN:
                this.preferredResultSetFetchDirection = OrientJdbcResultSet.DEFAULT_FETCH_DIRECTION;
                break;
            default:
                throw new SQLException(ErrorMessages.get("ResultSet.badFetchDirection",
                        ResultSet.FETCH_FORWARD + ", " + ResultSet.FETCH_REVERSE + ", " + 
                        ResultSet.FETCH_UNKNOWN, direction));
	    }
	}

	public void setFetchSize(final int rows) throws SQLException {
	    if (closed)
            throw new SQLException(ErrorMessages.get("ResultSet.setFetchSizeOnClosedObject", MESSAGE_FORMAT_CHOICE_INDEX));
        if (rows < 0)
            throw new SQLException(ErrorMessages.get("ResultSet.badFetchSize", rows));
        preferredResultSetFetchSize = rows;
	}

	public void setMaxFieldSize(final int max) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void setMaxRows(final int max) throws SQLException {
	    if (closed)
            throw new SQLException(ErrorMessages.get("ResultSet.setMaxRowsOnClosedObject",MESSAGE_FORMAT_CHOICE_INDEX));
        if (max < 0)
            throw new SQLException(ErrorMessages.get("ResultSet.badMaxRows", max));
       this.preferredResultSetMaxRows = max;
	}

	public void setPoolable(final boolean poolable) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void setQueryTimeout(final int seconds) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		// This should check is this instance is a wrapper for the given class
		try {
			// the following if-then structure makes sense if the query can be a
			// subclass of OCommandSQL.
			if (this.statement == null)
    			// if the query instance is null, we use the class OCommandSQL
    			return OCommandSQL.class.isAssignableFrom(iface);
			else return this.statement.getClass().isAssignableFrom(iface);
		} catch (NullPointerException e) {
			throw new SQLException(e);
		}
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		// This should return the actual query object: OCommandSQL, OQuery, etc...
		try {
			return iface.cast(statement);
		} catch (ClassCastException e) {
			throw new SQLException(e);
		}
	}

}
