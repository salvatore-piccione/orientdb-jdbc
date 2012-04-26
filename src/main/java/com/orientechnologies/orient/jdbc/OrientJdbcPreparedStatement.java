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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLSelect;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * TODO Add authors
 * @author Salvatore Piccione (TXT e-solutions SpA - salvatore.piccione AT network.txtgroup.com)
 *
 */
public class OrientJdbcPreparedStatement extends OrientJdbcStatement implements PreparedStatement {

	private static final int MESSAGE_FORMAT_CHOICE_INDEX = 1;
	private List<String> params;
	private List<Object[]> batchParams;

	public OrientJdbcPreparedStatement(OrientJdbcConnection iConnection, String sql, boolean readOnly) throws SQLException {
		super(iConnection,readOnly);
		init(sql);
	}
	
	public OrientJdbcPreparedStatement(OrientJdbcConnection iConnection, String sql,
			boolean readOnly, int resultSetType, int resultSetConcurrency) throws SQLException{
		super(iConnection, resultSetType, resultSetConcurrency, readOnly);
		init(sql);
	}

	public OrientJdbcPreparedStatement(OrientJdbcConnection iConnection, String sql,
			boolean readOnly, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		super(iConnection, resultSetType, resultSetConcurrency, resultSetHoldability, readOnly);
		init(sql);
	}
	
	private void init(String sql) throws SQLException {
	    if ("".equals(sql))
            throw new SQLException(ErrorMessages.get("PreparedStatement.compileEmptyString"));
        if (sql.startsWith(OCommandExecutorSQLSelect.KEYWORD_SELECT))
		    statement = new OSQLSynchQuery<ODocument>(sql);
		else
		    statement = new OCommandSQL(sql);
        params = new LinkedList<String>();
        batchParams = new LinkedList<Object[]>();
	}

	@SuppressWarnings("unchecked")
    private void executeOQuery () throws SQLException {
	    if (closed)
            throw new SQLException(ErrorMessages.get("Statement.executeQueryOnClosedObject",MESSAGE_FORMAT_CHOICE_INDEX, statement.toString()));
	    updateCount = NO_UPDATE_RESULT;
	    if (statement instanceof OQuery) {
            try {
                List<ODocument> documents = database.query((OQuery<ODocument>) statement,params.toArray(new Object[params.size()]));
                updateCount = NO_UPDATE_RESULT;
                if (preferredResultSetMaxRows == NO_LIMIT)
                    resultSet = new OrientJdbcResultSet(this, documents, resultSetType, resultSetConcurrency, resultSetHoldability, preferredResultSetFetchDirection, true);
                else if (documents.size() > preferredResultSetMaxRows)
                    resultSet = new OrientJdbcResultSet(this, documents.subList(0, preferredResultSetMaxRows), resultSetType, resultSetConcurrency, resultSetHoldability, preferredResultSetFetchDirection, true);
                else
                    resultSet = new OrientJdbcResultSet(this, documents, resultSetType, resultSetConcurrency, resultSetHoldability, preferredResultSetFetchDirection, true);
    
            } catch (OQueryParsingException e) {
                throw new SQLSyntaxErrorException("Error on parsing the query", e);
            }
	    } else
	        throw new SQLException(ErrorMessages.get("Statement.executeUpdateAsQuery", statement.toString()));
	}

	@Override
	public void close() throws SQLException {
		super.close();
		if (!closed) {
			this.batchParams = null;
			this.params = null;
		}
	}

	public ResultSet executeQuery() throws SQLException {
	    executeOQuery();
	    return resultSet;
	}

	public int executeUpdate() throws SQLException {
        if (closed)
            throw new SQLException(ErrorMessages.get("Statement.executeStatementOnClosedObject", MESSAGE_FORMAT_CHOICE_INDEX, statement.toString()));
	    super.executeUpdate(params.toArray(new Object[params.size()]));
		return updateCount;
	}
	
	private void setParameter (int parameterIndex, String value, String className) throws SQLException {
        if (closed)
            throw new SQLException(ErrorMessages.get("PreparedStatement.setParameterOnClosedObject", parameterIndex,className));
	    if (parameterIndex < 1)
	        throw new SQLException (ErrorMessages.get("PreparedStatement.parameterIndexLessThanMin"));
        if (parameterIndex > params.size()) {
            while (parameterIndex < params.size())
                params.add(null);
            params.add(value);
        } else {
            int index = parameterIndex - 1; 
            params.remove(index);
            params.add(index, value);
        }
	}

	public void setNull(int parameterIndex, int sqlType) throws SQLException {
	    //FIXME: does OrientDB support null parameters?
	    throw new SQLFeatureNotSupportedException();
	}

	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		setParameter(parameterIndex, Boolean.toString(x), Boolean.class.getName());
	}

	public void setByte(int parameterIndex, byte x) throws SQLException {
	    setParameter(parameterIndex, Byte.toString(x), Byte.class.getName());
	}

	public void setShort(int parameterIndex, short x) throws SQLException {
	    setParameter(parameterIndex, Short.toString(x), Short.class.getName());
	}

	public void setInt(int parameterIndex, int x) throws SQLException {
	    setParameter(parameterIndex, Integer.toString(x), Integer.class.getName());
	}

	public void setLong(int parameterIndex, long x) throws SQLException {
	    setParameter(parameterIndex, Long.toString(x), Long.class.getName());
	}

	public void setFloat(int parameterIndex, float x) throws SQLException {
	    setParameter(parameterIndex, Float.toString(x), Float.class.getName());
	}

	public void setDouble(int parameterIndex, double x) throws SQLException {
	    setParameter(parameterIndex, Double.toString(x), Double.class.getName());
	}

	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
	    setParameter(parameterIndex, x.toPlainString(), BigDecimal.class.getName());
	}

	public void setString(int parameterIndex, String x) throws SQLException {
	    setParameter(parameterIndex, x, String.class.getName());
	}

	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void setDate(int parameterIndex, Date x) throws SQLException {
	    setParameter(parameterIndex, new SimpleDateFormat(ORecordSerializerJSON.DEF_DATE_FORMAT).format(x), Date.class.getName());
	}

	public void setTime(int parameterIndex, Time x) throws SQLException {
	    //TODO TEST!
	    setParameter(parameterIndex, new SimpleDateFormat(
	            ORecordSerializerJSON.DEF_DATE_FORMAT.substring(ORecordSerializerJSON.DEF_DATE_FORMAT.indexOf(' ')))
	            .format(x), Time.class.getName());
	}

	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
	    setParameter(parameterIndex, new SimpleDateFormat(ORecordSerializerJSON.DEF_DATE_FORMAT).format(x), Timestamp.class.getName());
	}

	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void clearParameters() throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("PreparedStatement.clearParameterOnClosedObject"));
		params.clear();
	}

	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void setObject(int parameterIndex, Object x) throws SQLException {
	    setParameter(parameterIndex, x.toString(), Object.class.getName());
	}

	public boolean execute() throws SQLException {
	    if (statement instanceof OSQLSynchQuery) {
	        executeOQuery();
	        return true;
	    } else {
	        this.executeUpdate();
	        return false;
	    }
	}

    public void addBatch() throws SQLException {
        if (closed)
            throw new SQLException(ErrorMessages.get("PreparedStatement.addBatchParametersOnClosedObject"));
        this.batchParams.add(params.toArray(new Object[params.size()]));
        params.clear();
	}

	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public ResultSetMetaData getMetaData() throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("ResultSet.getMetadataFromClosedObject", MESSAGE_FORMAT_CHOICE_INDEX));
	    if (resultSet == null)
	        return null;
	    else
	        return resultSet.getMetaData();
	}

	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        DateFormat formatter = new SimpleDateFormat(ORecordSerializerJSON.DEF_DATE_FORMAT);
        formatter.setCalendar(cal);
        setParameter(parameterIndex, formatter.format(x), Date.class.getName() + " + " + Calendar.class.getName());
	}

	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        DateFormat formatter = new SimpleDateFormat(
                ORecordSerializerJSON.DEF_DATE_FORMAT.substring(ORecordSerializerJSON.DEF_DATE_FORMAT.indexOf(' ')));
        formatter.setCalendar(cal);
        setParameter(parameterIndex, formatter.format(x), Time.class.getName() + " + " + Calendar.class.getName());
	}

	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        DateFormat formatter = new SimpleDateFormat(ORecordSerializerJSON.DEF_DATE_FORMAT);
        formatter.setCalendar(cal);
        setParameter(parameterIndex, formatter.format(x), Timestamp.class.getName() + " + " + Calendar.class.getName());
	}

	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void setURL(int parameterIndex, URL x) throws SQLException {
	    setParameter(parameterIndex, x.toString(), URL.class.getName());
	}

	public ParameterMetaData getParameterMetaData() throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void setRowId(int parameterIndex, RowId x) throws SQLException {
	    setParameter(parameterIndex, x.toString(), RowId.class.getName());
	}

	public void setNString(int parameterIndex, String value) throws SQLException {
	    setParameter(parameterIndex, value, "N" + String.class.getName());
	}

	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void setNClob(int parameterIndex, NClob value) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

    public void clearBatch() throws SQLException {
        super.clearBatch();
        batchParams.clear();
    }
    
    public int[] executeBatch() throws SQLException {
        if (closed)
            throw new SQLException(ErrorMessages.get("Statement.executeBatchOnClosedObject"));
        
        ODatabaseRecordThreadLocal.INSTANCE.set(database);
        
        //reset the fields holding the result of single statement
        resultSet = null;
        updateCount = NO_UPDATE_RESULT;
        
        int[] results = new int[batches.size()];
        int i = 0;
        try {
            for (OCommandSQL sql : batches) {
                results[i] = super.executeBatchUpdate(sql, batchParams.get(i));
            	i++;
            }
        } catch (Exception e) {
            throw new BatchUpdateException(e);
        }
        return results;
    }
}
