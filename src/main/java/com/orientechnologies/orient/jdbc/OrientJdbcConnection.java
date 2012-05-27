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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;

/**
 * TODO Add authors name
 * @author Salvatore Piccione (TXT e-solutions SpA - salvatore.piccione AT network.txtgroup.com)
 *
 */
public abstract class OrientJdbcConnection implements Connection, ConnectionSpecificMetaData {
	//see ML discussion: https://groups.google.com/d/topic/orient-database/VmXkPlD74pA/discussion
	//Dirty reads are always prevented but phantom and non-repeatable reads can occur
	//because transactions don't lock reads
    static final int DEFAULT_TRANSACTION_ISOLATION = Connection.TRANSACTION_READ_COMMITTED;

	private static final int MESSAGE_FORMAT_CHOICE_INDEX = 3;
	
	private boolean readOnly = false;
	protected boolean autoCommit;
	private int resultSetHoldability = OrientJdbcResultSet.DEFAULT_HOLDABILITY;

	protected abstract ODatabaseDocumentTx getOrientDatabase ();

	public void clearWarnings() throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void close() throws SQLException {
		closeImpl();
	}
	
	protected abstract void closeImpl() throws SQLException;

	public boolean isClosed() throws SQLException {
		return getOrientDatabase().isClosed();
	}

	public boolean isReadOnly() throws SQLException {
		if (isClosed() == true)
			throw new SQLException(ErrorMessages.get("Connection.getReadOnlyFlagFromClosedObject"));
		return readOnly;
	}

	public void setReadOnly(boolean iReadOnly) throws SQLException {
		if (isClosed() == true)
			throw new SQLException(ErrorMessages.get("Connection.setReadOnlyFlagOnClosedObject"));
		readOnly = iReadOnly;
	}

	public boolean isValid(int timeout) throws SQLException {
		return this.isClosed();
	}

	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
	    throw new SQLException(ErrorMessages.get("Connection.arrayCreationNotSupported"), 
            XOpenSqlStates.FEATURE_NOT_SUPPORTED);
	}

	public Blob createBlob() throws SQLException {
	    //at the moment the filling of an OrientBlob instance is not supported
	    throw new SQLFeatureNotSupportedException();
	}

	public Clob createClob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public NClob createNClob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public SQLXML createSQLXML() throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public Statement createStatement() throws SQLException {
		if (isClosed())
			throw new SQLException(ErrorMessages.get("Connection.statementCreationOnClosedObject"));
		return new OrientJdbcStatement(this, readOnly);
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		if (isClosed())
			throw new SQLException(ErrorMessages.get("Connection.statementCreationOnClosedObject"));
		return new OrientJdbcStatement(this, resultSetType, resultSetConcurrency, readOnly);
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		if (isClosed())
			throw new SQLException(ErrorMessages.get("Connection.statementCreationOnClosedObject"));
		return new OrientJdbcStatement(this, resultSetType, resultSetConcurrency, resultSetHoldability, readOnly);
	}

	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
	    throw new SQLFeatureNotSupportedException(); 
	}

	public boolean getAutoCommit() throws SQLException {
		if (isClosed())
			throw new SQLException(ErrorMessages.get("Connection.getAutoCommitFlagFromClosedObject"));
		return autoCommit;
	}

	public String getCatalog() throws SQLException {
		if (isClosed())
			throw new SQLException(ErrorMessages.get("Connection.getCatalogFromClosedObject"));
		return getOrientDatabase().getName();
	}

	public Properties getClientInfo() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public String getClientInfo(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public int getHoldability() throws SQLException {
		if (isClosed())
			throw new SQLException(ErrorMessages.get("ResultSet.getHoldabilityFromClosedObject",MESSAGE_FORMAT_CHOICE_INDEX));
		return resultSetHoldability;
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		if (isClosed())
			throw new SQLException(ErrorMessages.get("Connection.getDatabaseMetaDataFromClosedObject"));
		return new OrientJdbcDatabaseMetaData(this);
	}

	public int getTransactionIsolation() throws SQLException {
	    if (isClosed())
			throw new SQLException(ErrorMessages.get("Connection.getTransactionIsolationFromClosedObject"));
		return DEFAULT_TRANSACTION_ISOLATION;
	}

	public Map<String, Class<?>> getTypeMap() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public SQLWarning getWarnings() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public String nativeSQL(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		if (isClosed())
			throw new SQLException(ErrorMessages.get("Connection.preparedStatementCreationOnClosedObject", sql));
		return new OrientJdbcPreparedStatement(this, sql, readOnly);
	}

	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		if (isClosed())
			throw new SQLException(ErrorMessages.get("Connection.preparedStatementCreationOnClosedObject", sql));
		if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS)
			return new OrientJdbcPreparedStatement(this, sql, readOnly);
		else if (autoGeneratedKeys == Statement.RETURN_GENERATED_KEYS)
			throw new SQLFeatureNotSupportedException();
		else
			throw new SQLException(ErrorMessages.get("Statement.badGeneratedKeysFlag", autoGeneratedKeys, 
					Statement.NO_GENERATED_KEYS + ", " + Statement.RETURN_GENERATED_KEYS));
	}

	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		if (isClosed())
			throw new SQLException(ErrorMessages.get("Connection.preparedStatementCreationOnClosedObject", sql));
		return new OrientJdbcPreparedStatement(this, sql, readOnly, resultSetType, resultSetConcurrency);
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		if (isClosed())
			throw new SQLException(ErrorMessages.get("Connection.preparedStatementCreationOnClosedObject", sql));
		return new OrientJdbcPreparedStatement(this, sql, readOnly, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void rollback(Savepoint savepoint) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setAutoCommit(boolean autoCommit) throws SQLException {
	    if (isClosed())
	        throw new SQLException(ErrorMessages.get("Connection.setAutoCommitOnClosedObject"));
	    //silently commit the underlying transaction if the autoCommit was disabled and has been enabled
	    if (autoCommit != this.autoCommit) {
		    if (autoCommit && !this.autoCommit)
		    	commit();
		    else if (this.autoCommit && !autoCommit)
		    	configTransaction();
			this.autoCommit = autoCommit;
	    }   
	}
	
	protected abstract void configTransaction();

	public void setCatalog(String catalog) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		throw new SQLClientInfoException(ErrorMessages.get("Connection.setClientInfo"),null);
	}

	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		throw new SQLClientInfoException(ErrorMessages.get("Connection.setClientInfo"),null);
	}

	public void setHoldability(int holdability) throws SQLException {
	    throw new SQLException(ErrorMessages.get("Connection.setHoldabilityNotSupported"),
	            XOpenSqlStates.FEATURE_NOT_SUPPORTED);
	}

	public Savepoint setSavepoint() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Savepoint setSavepoint(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setTransactionIsolation(int level) throws SQLException {
		//TODO TO BE IMPLEMENTED
		throw new SQLFeatureNotSupportedException();
	}

	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}
	
	protected abstract boolean isWrapperForImpl (Class<?> iface);
	
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
        if (iface == null)
            throw new SQLException(ErrorMessages.get("Wrapper.wrappedClassIsNull"));
        return isWrapperForImpl(iface);
    }

	protected abstract  <T> T unwrapImpl(Class<T> iface) throws SQLException, ClassCastException;
    
	public <T> T unwrap(Class<T> iface) throws SQLException {
		try {
	        if (iface == null)
	            throw new SQLException(ErrorMessages.get("Wrapper.wrappedClassIsNull"));
	        return unwrapImpl(iface);
		} catch (ClassCastException e) {
			throw new SQLException(ErrorMessages.get("Connection.cannotUnwrap",
            		ODatabaseRecord.class.getName(), this.getClass().getName()));
		}
    }
	public String getUrl() {
		return getOrientDatabase().getURL();
	}
	
	/* (non-Javadoc)
     * @see java.sql.Connection#commit()
     */
    public void commit() throws SQLException {
        if (isClosed())
            throw new SQLException(ErrorMessages.get("Connection.commitOnClosedObject"));
        if (autoCommit)
            throw new SQLException(ErrorMessages.get("Connection.commitOnEnabledAutoCommit"));
        this.commitImpl();
    }
    
    protected abstract void commitImpl ();

    @Override
    public void rollback() throws SQLException {
        if (isClosed())
            throw new SQLException(ErrorMessages.get("Connection.rollbackOnClosedObject"));
        if (autoCommit)
            throw new SQLException(ErrorMessages.get("Connection.rollbackOnEnabledAutoCommit"));
        this.rollabackImpl();
    }
    
    protected abstract void rollabackImpl ();

	@Override
	public boolean supportsTransactionIsolationLevel(int level)
			throws SQLException {
		//see ML discussion: https://groups.google.com/d/topic/orient-database/VmXkPlD74pA/discussion
    	switch (level) {
    		case Connection.TRANSACTION_NONE:
    		case Connection.TRANSACTION_READ_COMMITTED:
    			return true;
    		case Connection.TRANSACTION_READ_UNCOMMITTED:
    		case Connection.TRANSACTION_REPEATABLE_READ:
    		case Connection.TRANSACTION_SERIALIZABLE:
    			return false;
			default:
				throw new SQLException(ErrorMessages.get("Connection.badTransactionIsolationlevel",
						Connection.TRANSACTION_NONE + ", " + 
						Connection.TRANSACTION_READ_COMMITTED + ", " +
						Connection.TRANSACTION_READ_UNCOMMITTED + ", " + 
						Connection.TRANSACTION_REPEATABLE_READ + ", " +
						Connection.TRANSACTION_SERIALIZABLE, level));
    	}
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsResultSetConcurrency(int type, int concurrency)
			throws SQLException {
		if (supportsResultSetType(type)) {
            switch (concurrency) {
                case ResultSet.CONCUR_READ_ONLY:
                    return true;
                //TODO to be implemented!
                case ResultSet.CONCUR_UPDATABLE:
                    return false; 
                default:
                    throw new SQLException(ErrorMessages.get(
                            "ResultSet.badConcurrency", ResultSet.CONCUR_READ_ONLY + ", " + ResultSet.CONCUR_UPDATABLE, concurrency));
            }
        } else
            return false;
	}

	@Override
	public boolean supportsResultSetType(int type) throws SQLException {
		//see ML discussion: https://groups.google.com/d/topic/orient-database/VmXkPlD74pA/discussion
        switch (type) {
            case ResultSet.TYPE_FORWARD_ONLY:
            case ResultSet.TYPE_SCROLL_INSENSITIVE:
                return true;
        	//TODO TO BE IMPLEMENTED
            case ResultSet.TYPE_SCROLL_SENSITIVE:
            	return false;
            default:
                throw new SQLException(ErrorMessages.get("ResultSet.badType",ResultSet.TYPE_FORWARD_ONLY + ", " + 
                        ResultSet.TYPE_SCROLL_INSENSITIVE + ", " + ResultSet.TYPE_SCROLL_SENSITIVE, type));
        }
	}

	@Override
	public boolean supportsResultSetHoldability(int holdability)
			throws SQLException {
		switch (holdability) {
        case ResultSet.HOLD_CURSORS_OVER_COMMIT:
            return true; //this is the default behavior
        //TODO to be implemented!
        case ResultSet.CLOSE_CURSORS_AT_COMMIT:
            return false;
        default:
            throw new SQLException(ErrorMessages.get(
                    "ResultSet.badHoldability", ResultSet.CLOSE_CURSORS_AT_COMMIT + ", " + ResultSet.HOLD_CURSORS_OVER_COMMIT, holdability));
		}	
	}
}
