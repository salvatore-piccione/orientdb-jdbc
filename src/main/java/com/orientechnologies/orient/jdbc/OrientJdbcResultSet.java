/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 * Copyright 2011-2012 CELI srl
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
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLAbstract;
import com.orientechnologies.orient.jdbc.common.OrientJdbcConstants;

/**
 * @author Roberto Franchini (CELI srl - franchin--at--celi.it)
 * @author Salvatore Piccione (TXT e-solutions SpA - salvatore.piccione AT network.txtgroup.com)
 */
public class OrientJdbcResultSet implements ResultSet {
    /*
     * The column name to be used to get the rid of the underlying record
     */
    private static final String RID_COLUMN_NAME = OCommandExecutorSQLAbstract.KEYWORD_RID;
    
    private static final int MESSAGE_FORMAT_CHOICE_INDEX = 2;
    
    private static final int BEFORE_FIRST_INDEX = -1;
    
    static final int DEFAULT_FETCH_DIRECTION = ResultSet.FETCH_FORWARD;
    static final int DEFAULT_FETCH_SIZE = OrientJdbcConstants.UNLIMITED_FETCH_SIZE;
    
	private List<ODocument> records = null;
	private OrientJdbcStatement statement;
	private OrientJdbcDatabaseMetaData databaseMetaData;
	//this is the actual index starting from 0
	private int cursor;
	private int rowCount;
	private ODocument document;
	private String[] fieldNames;
	private int type;
	private int concurrency;
	private int holdability;
	private boolean closed;
	
	private int fetchDirection;
	private int fetchSize;
	
	private String currentColumnLabel;

	OrientJdbcResultSet(OrientJdbcStatement iOrientJdbcStatement, List<ODocument> iRecords, 
	        int type, int concurrency, int holdability, int fetchDirection, boolean includeRid) throws SQLException {
	    statement = iOrientJdbcStatement;
		databaseMetaData = (OrientJdbcDatabaseMetaData) statement.getConnection().getMetaData();
		records = iRecords;
		rowCount = iRecords.size();
		
		setFetchDirection(fetchDirection);
		//set up the cursor either to the before-first or after-last position
		//according to the fetch direction
		if (fetchDirection == FETCH_REVERSE)
		    cursor = rowCount;
		else
		    cursor = BEFORE_FIRST_INDEX;

		fetchSize = rowCount;
		document = null;
		
		if (rowCount > 0) {
			ODocument document = records.get(0);
			ODatabaseRecordThreadLocal.INSTANCE.set(document.getDatabase());

            String[] fieldNames = document.fieldNames();
			if (includeRid) {
    			//check that the field name list has the RID
    			boolean hasRIDField = false;
    			int i = 0;
    			while (i < fieldNames.length && !hasRIDField) {
    			    if (RID_COLUMN_NAME.equals(fieldNames[i]))
    			        hasRIDField = true;
    			    else
    			        i++;
    			}
    			if (hasRIDField)
    			    this.fieldNames = fieldNames;
    			else {
    			    //add to the top of the field name list the "rid" field
    	            //that allows the retrieval of the document rid as this field
    	            //is not included in ODocument.fieldNames()
    	            ArrayList<String> fieldNameList = new ArrayList<String> (fieldNames.length + 1);
    	            fieldNameList.add(RID_COLUMN_NAME);
    	            fieldNameList.addAll(Arrays.asList(fieldNames));
    	            this.fieldNames = fieldNameList.toArray(new String[fieldNames.length + 1]);
    			}
			} else
			    this.fieldNames = fieldNames;
			    
		}
		if  (databaseMetaData.supportsResultSetType(type))
		    this.type = type;
		else 
		    throw new SQLException(ErrorMessages.get("ResultSet.unsupportedType", type, statement.getConnection().getClass().getName()));

		if (databaseMetaData.supportsResultSetConcurrency(type, concurrency))
		    this.concurrency = concurrency;
		else
		    throw new SQLException(ErrorMessages.get("ResultSet.unsupportedConcurrency", concurrency, statement.getConnection().getClass().getName()));

		if (databaseMetaData.supportsResultSetHoldability(holdability))
		    this.holdability = holdability;
		else
		    throw new SQLException(ErrorMessages.get("ResultSet.unsopportedHoldability", holdability, statement.getConnection().getClass().getName()));
		
		closed = false;
	}

//	private void setDatabaseOnThreadLocalInstance() {
//		ODatabaseRecordThreadLocal.INSTANCE.set(document.getDatabase());
//	}

	public void close() throws SQLException {
	    if (!closed) {
    		rowCount = 0;
    		records = null;
    		document = null;
    		statement = null;
    		closed = true;
	    }
	}

	public boolean first() throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("ResultSet.moveToFirstOnClosedObject"));
	    if (holdability == TYPE_FORWARD_ONLY)
	        throw new SQLException(ErrorMessages.get("ResultSet.moveToFirstWhenForwardOnly"));
	    
	    if (fetchDirection == FETCH_REVERSE)
            return moveToAbsolutePosition(rowCount - 1);
	    else
            return moveToAbsolutePosition(0);
	}

	public boolean last() throws SQLException {
        if (closed)
            throw new SQLException(ErrorMessages.get("ResultSet.moveToLastOnClosedObject"));
        if (holdability == TYPE_FORWARD_ONLY)
            throw new SQLException(ErrorMessages.get("ResultSet.moveToLastWhenForwardOnly"));
        
        if (fetchDirection == FETCH_REVERSE)
            return moveToAbsolutePosition(0);
        else
            return moveToAbsolutePosition(rowCount - 1);
	}

	public boolean next() throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("ResultSet.moveToNextOnClosedObject"));
	    
	    if (fetchDirection == FETCH_REVERSE)
	        return moveToAbsolutePosition(--cursor);
	    else
	        return moveToAbsolutePosition(++cursor);
	}

	public boolean previous() throws SQLException {
        if (closed)
            throw new SQLException(ErrorMessages.get("ResultSet.moveToPreviousOnClosedObject"));
        if (fetchDirection == FETCH_FORWARD)
            throw new SQLException(ErrorMessages.get("ResultSet.moveToPreviousWhenForwardOnly"));
        
        if (fetchDirection == FETCH_REVERSE)
            return moveToAbsolutePosition(++cursor);
        else
            return moveToAbsolutePosition(--cursor);
	}

	public void afterLast() throws SQLException {
        if (closed)
            throw new SQLException(ErrorMessages.get("ResultSet.moveToAfterLastOnClosedObject"));
        if (type == TYPE_FORWARD_ONLY)
            throw new SQLException(ErrorMessages.get("ResultSet.moveToAfterLastWhenForwardOnly"));

        // OUT OF LAST ITEM
        if (fetchDirection == FETCH_REVERSE)
            cursor = BEFORE_FIRST_INDEX;
        else
            cursor = rowCount;
	}

	public void beforeFirst() throws SQLException {
        if (closed)
            throw new SQLException(ErrorMessages.get("ResultSet.moveToBeforeFirstOnClosedObject"));
        if (type == TYPE_FORWARD_ONLY)
            throw new SQLException(ErrorMessages.get("ResultSet.moveToBeforeFirstWhenForwardOnly"));
		
        // OUT OF FIRST ITEM
        if (fetchDirection == FETCH_REVERSE)
            cursor = rowCount;
        else
            cursor = BEFORE_FIRST_INDEX;
	}

	public boolean relative(int iRows) throws SQLException {
        if (closed)
            throw new SQLException(ErrorMessages.get("ResultSet.moveToRelativePosOnClosedObject"));
        if (type == TYPE_FORWARD_ONLY)
            throw new SQLException(ErrorMessages.get("ResultSet.moveToRelativePostWhenForwardOnly"));
        
        if (fetchDirection == FETCH_REVERSE)
            return moveToAbsolutePosition(cursor - iRows);
        else
            return moveToAbsolutePosition(cursor + iRows);
	}

	public boolean absolute(int iRowNumber) throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("ResultSet.moveToAbsolutePosOnClosedObject", iRowNumber));
	    //no matter about the fetch direction...
	    if (iRowNumber > 0)
	        return moveToAbsolutePosition(iRowNumber - 1);
	    else if (iRowNumber < 0)
	        return moveToAbsolutePosition(iRowNumber);
	    else {
	        //iRowNumber == 0 -> beforeFirst
	        //pass a negative number greater than the rowCount
	        //so that the delta against the rowCount is negative
	        //and the cursor is set to before-first
	        return moveToAbsolutePosition(-rowCount - 1);
	    }
	}
	
	private boolean moveToAbsolutePosition (int rowIndex) throws SQLException {
        if (rowIndex >= rowCount) {
            // OUT OF LAST ITEM
            cursor = rowCount;
            return false;
        } else if (rowIndex >= 0) {
            cursor = rowIndex;
            document = records.get(cursor);
            return true;
        } else {
            //rowIndex < 0
            if (type == FETCH_FORWARD)
                throw new SQLException(ErrorMessages.get("ResultSet.badAbsolutePosOnFetchForward",rowIndex));
            int posAfterRotation = rowCount +rowIndex;
            if (posAfterRotation < 0) {
                // OUT OF FIRST ITEM
                cursor = BEFORE_FIRST_INDEX;
                return false;
            } else {
                cursor = posAfterRotation;
                document = records.get(cursor);
                return true;
            }
        }
	}

	public boolean isAfterLast() throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("ResultSet.getAfterLastFlagFromClosedObject"));
	    
	    if (fetchDirection == FETCH_REVERSE)
	        return cursor == BEFORE_FIRST_INDEX;
	    else
	        return cursor == rowCount;
	}

	public boolean isBeforeFirst() throws SQLException {
        if (closed)
            throw new SQLException(ErrorMessages.get("ResultSet.getBeforeFirstFlagFromClosedObject"));
        
	    if (fetchDirection == FETCH_REVERSE)
	        return cursor == rowCount;
	    else
	        return cursor == BEFORE_FIRST_INDEX;
	}

	public boolean isClosed() throws SQLException {
		return closed;
	}

	public boolean isFirst() throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("ResultSet.getFirstFlagFromClosedObject"));
	    
	    if (fetchDirection == FETCH_REVERSE)
	        return cursor == rowCount - 1;
	    else
	        return cursor == 0;
	}

	public boolean isLast() throws SQLException {
        if (closed)
            throw new SQLException(ErrorMessages.get("ResultSet.getLastFlagFromClosedObject"));
        
        if (fetchDirection == FETCH_REVERSE)
            return cursor == 0;
        else
            return cursor == rowCount - 1;
	}

	public Statement getStatement() throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("ResultSet.getStatementFromClosedObject"));
		return statement;
	}

	public ResultSetMetaData getMetaData() throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("ResultSet.getMetaDataFromClosedObject"));
		return new OrientJdbcResultSetMetaData(this);
	}

	public void deleteRow() throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("ResultSet.deleteRowFromClosedObject"));
		document.delete();
	}

	public int findColumn(String columnLabel) throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("ResultSet.findColumnInClosedObject"));
		int column = 0;
		int i = 0;
		while (i < (fieldNames.length - 1) && column == 0) {
			if (fieldNames[i].equals(columnLabel)) column = i + 1;
			else i++;
		}
		if (column == 0)
		    throw new SQLException(ErrorMessages.get("ResultSet.missingColumnLabel", columnLabel));
		return column;
	}

	private int getFieldIndex(int columnIndex) throws SQLException {
		if (columnIndex < 1)
		    throw new SQLException(ErrorMessages.get("ResultSet.columnIndexLessThanMin", columnIndex));
		if (columnIndex > fieldNames.length)
		    throw new SQLException(ErrorMessages.get("ResultSet.columnIndexGreaterThanMax", fieldNames.length, columnIndex));
		return columnIndex - 1;
	}

	public Array getArray(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Array getArray(String columnLabel) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public InputStream getAsciiStream(int columnIndex) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public InputStream getAsciiStream(String columnLabel) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
	    return getBigDecimal(fieldNames[getFieldIndex(columnIndex)]);
	}

	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
	    if (closed)
            throw new SQLException(ErrorMessages.get("ResultSet.getItemByLabelFromClosedObject",BigDecimal.class.getName(),columnLabel));
		currentColumnLabel = columnLabel;
	    try {
			ODatabaseRecordThreadLocal.INSTANCE.set(document.getDatabase());
	        return document.field(columnLabel, BigDecimal.class);
	    } catch (Exception e) {
	        throw new SQLException(ErrorMessages.get("ResultSet.getItem", columnLabel, BigDecimal.class.getName(), cursor + 1),e);
	    }
	}

	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
	    return getBigDecimal(fieldNames[getFieldIndex(columnIndex)], scale);
	}

	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
	    if (closed)
            throw new SQLException(ErrorMessages.get("ResultSet.getItemByLabelFromClosedObject",BigDecimal.class.getName(),columnLabel));
		currentColumnLabel = columnLabel;
        try {
			ODatabaseRecordThreadLocal.INSTANCE.set(document.getDatabase());
            BigDecimal originalValue = document.field(columnLabel, BigDecimal.class);
            if (originalValue == null)
                return null;
            if (originalValue.scale() == scale)
                return originalValue;
            return originalValue.setScale(scale);
        } catch (Exception e) {
            throw new SQLException(ErrorMessages.get("ResultSet.getItem", columnLabel, BigDecimal.class.getName(), cursor + 1),e);
        }
	}

	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		return getBinaryStream(fieldNames[getFieldIndex(columnIndex)]);
	}

	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("ResultSet.getItemByLabelFromClosedObject", InputStream.class.getName(), columnLabel));
		try {
			Blob blob = getOrientBlob(columnLabel);
			if (blob == null) return null;
			return blob.getBinaryStream();
		} catch (Exception e) {
			throw new SQLException(ErrorMessages.get("ResultSet.getItem", columnLabel, InputStream.class.getName(), cursor + 1),e);
		}
	}

	public Blob getBlob(int columnIndex) throws SQLException {
		return getBlob(fieldNames[getFieldIndex(columnIndex)]);
	}

	public Blob getBlob(String columnLabel) throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("ResultSet.getItemByLabelFromClosedObject", Blob.class.getName(), columnLabel));
		try {
			return getOrientBlob(columnLabel);
		} catch (Exception e) {
			throw new SQLException(ErrorMessages.get("ResultSet.getItem", columnLabel, Blob.class.getName(), cursor + 1),e);
		}
	}
	
	private Blob getOrientBlob (String columnLabel) {
		ODatabaseRecordThreadLocal.INSTANCE.set(document.getDatabase());
		currentColumnLabel = columnLabel;
		Object value = document.field(columnLabel);
		if (value instanceof ORecordBytes) {
			return new OrientBlob((ORecordBytes) value);
		}
		if (value instanceof ORecordLazyList) {
			ORecordLazyList list = (ORecordLazyList) value;
			// check if all the list items are instances of ORecordBytes
			ListIterator<OIdentifiable> iterator = list.listIterator();

			List<ORecordBytes> binaryRecordList = new ArrayList<ORecordBytes>(list.size());
			while (iterator.hasNext()) {
				OIdentifiable listElement = iterator.next();
				ORecordBytes ob = document.getDatabase().load(listElement.getIdentity());
				binaryRecordList.add(ob);
			}
			return new OrientBlob(binaryRecordList);
		}
		return null;
	}

	public boolean getBoolean(int columnIndex) throws SQLException {
		return getBoolean(fieldNames[getFieldIndex(columnIndex)]);
	}

	@SuppressWarnings("boxing")
	public boolean getBoolean(String columnLabel) throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("ResultSet.getItemByLabelFromClosedObject", Boolean.class.getName(), columnLabel));
		currentColumnLabel = columnLabel;
		try {
			ODatabaseRecordThreadLocal.INSTANCE.set(document.getDatabase());
			return (Boolean) document.field(columnLabel, OType.BOOLEAN);
		} catch (Exception e) {
			throw new SQLException(ErrorMessages.get("ResultSet.getItem", columnLabel, Boolean.class.getName(), cursor + 1),e);
		}

	}

	@SuppressWarnings("boxing")
	public byte getByte(int columnIndex) throws SQLException {
		return getByte(fieldNames[getFieldIndex(columnIndex)]);
	}

	public byte getByte(String columnLabel) throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("ResultSet.getItemByLabelFromClosedObject", Byte.class.getName(), columnLabel));
		currentColumnLabel = columnLabel;
		try {
			ODatabaseRecordThreadLocal.INSTANCE.set(document.getDatabase());
			return (Byte) document.field(columnLabel, OType.BYTE);
		} catch (Exception e) {
			throw new SQLException(ErrorMessages.get("ResultSet.getItem", columnLabel, Byte.class.getName(), cursor + 1),e);
		}
	}

	public byte[] getBytes(int columnIndex) throws SQLException {
		return getBytes(fieldNames[getFieldIndex(columnIndex)]);
	}

	public byte[] getBytes(String columnLabel) throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("ResultSet.getItemByLabelFromClosedObject", "byte[]", columnLabel));
		currentColumnLabel = columnLabel;
		try {
			ODatabaseRecordThreadLocal.INSTANCE.set(document.getDatabase());
			Object value = document.field(columnLabel);
			if (value == null)
				return null;
			if (value instanceof ORecordBytes) 
				return ((ORecordBytes) value).toStream();
			return document.field(columnLabel, OType.BINARY);
		} catch (Exception e) {
			throw new SQLException(ErrorMessages.get("ResultSet.getItem", columnLabel, "byte[]", cursor + 1),e);
		}
	}

	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Reader getCharacterStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Clob getClob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Clob getClob(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public int getConcurrency() throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("ResultSet.getConcurrencyFromClosedObject", MESSAGE_FORMAT_CHOICE_INDEX));
		return concurrency;
	}

	public String getCursorName() throws SQLException {
		//FIXME
		return getClass().getName();
	}

	public Date getDate(int columnIndex) throws SQLException {
		return getDate(fieldNames[getFieldIndex(columnIndex)]);
	}

	public Date getDate(String columnLabel) throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("ResultSet.getItemByLabelFromClosedObject", Date.class.getName(), columnLabel));
		currentColumnLabel = columnLabel;
		try {
			ODatabaseRecordThreadLocal.INSTANCE.set(document.getDatabase());
			java.util.Date date = document.field(columnLabel, OType.DATETIME);
			return new Date(date.getTime());
		} catch (Exception e) {
			throw new SQLException(ErrorMessages.get("ResultSet.getItem", columnLabel, Date.class.getName(), cursor + 1),e);
		}
	}

	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		return getDate(fieldNames[getFieldIndex(columnIndex)], cal);
	}

	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("ResultSet.getItemByLabelFromClosedObject", Date.class.getName() + " + " + Calendar.class.getName(), columnLabel));
	
		if (cal == null)
			throw new SQLException(ErrorMessages.get("ResultSet.calendarIsNull", columnLabel, cursor + 1));
		currentColumnLabel = columnLabel;
		try {
			ODatabaseRecordThreadLocal.INSTANCE.set(document.getDatabase());
			
			java.util.Date date = document.field(columnLabel, OType.DATETIME);
			if (date == null) return null;
			cal.setTimeInMillis(date.getTime());
			return new Date(cal.getTimeInMillis());
		} catch (Exception e) {
			throw new SQLException(ErrorMessages.get("ResultSet.getItem", columnLabel, Date.class.getName() + " + " + Calendar.class.getName(), cursor + 1),e);
		}
	}

	public double getDouble(int columnIndex) throws SQLException {
		return getDouble(fieldNames[getFieldIndex(columnIndex)]);
	}

	public double getDouble(String columnLabel) throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("ResultSet.getItemByLabelFromClosedObject", Double.class.getName(), columnLabel));
		currentColumnLabel = columnLabel;
		try {
			ODatabaseRecordThreadLocal.INSTANCE.set(document.getDatabase());
			return (Double) document.field(columnLabel, OType.DOUBLE);
		} catch (Exception e) {
			throw new SQLException(ErrorMessages.get("ResultSet.getItem", columnLabel, Double.class.getName() , cursor + 1),e);
		}
	}

	public int getFetchDirection() throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("ResultSet.getFetchDirectionFromClosedObject"));
		return fetchDirection;
	}

	public int getFetchSize() throws SQLException {
		if (closed)
            throw new SQLException(ErrorMessages.get("ResultSet.getFetchSizeFromClosedObject",MESSAGE_FORMAT_CHOICE_INDEX));
	    return fetchSize;
	}

	public float getFloat(int columnIndex) throws SQLException {
		return getFloat(fieldNames[getFieldIndex(columnIndex)]);
	}

	public float getFloat(String columnLabel) throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("ResultSet.getItemByLabelFromClosedObject", Float.class.getName(), columnLabel));
		currentColumnLabel = columnLabel;
		try {
			ODatabaseRecordThreadLocal.INSTANCE.set(document.getDatabase());
			return (Float) (document.field(columnLabel, OType.FLOAT));
		} catch (Exception e) {
			throw new SQLException(ErrorMessages.get("ResultSet.getItem", columnLabel, Float.class.getName() , cursor + 1),e);
		}
	}

	public int getHoldability() throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("ResultSet.getHoldabilityFromClosedObject", MESSAGE_FORMAT_CHOICE_INDEX));
		return holdability;
	}

	public int getInt(int columnIndex) throws SQLException {
		return getInt(fieldNames[getFieldIndex(columnIndex)]);
	}

	public int getInt(String columnLabel) throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("ResultSet.getItemByLabelFromClosedObject", Integer.class.getName(), columnLabel));
		currentColumnLabel = columnLabel;
		try {
			ODatabaseRecordThreadLocal.INSTANCE.set(document.getDatabase());
			return (Integer) document.field(columnLabel, OType.INTEGER);
		} catch (Exception e) {
			throw new SQLException(ErrorMessages.get("ResultSet.getItem", columnLabel, Integer.class.getName() , cursor + 1),e);
		}
	}

	public long getLong(int columnIndex) throws SQLException {
		return getLong(fieldNames[getFieldIndex(columnIndex)]);
	}

	public long getLong(String columnLabel) throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("ResultSet.getItemByLabelFromClosedObject", Long.class.getName(), columnLabel));
		currentColumnLabel = columnLabel;
		try {
			ODatabaseRecordThreadLocal.INSTANCE.set(document.getDatabase());
			return (Long) document.field(columnLabel, OType.LONG);
		} catch (Exception e) {
			throw new SQLException(ErrorMessages.get("ResultSet.getItem", columnLabel, Long.class.getName() , cursor + 1),e);
		}
	}

	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public NClob getNClob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public NClob getNClob(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public String getNString(int columnIndex) throws SQLException {
		return getString(fieldNames[getFieldIndex(columnIndex)]);
	}

	public String getNString(String columnLabel) throws SQLException {
		return getString(columnLabel);
	}

	public Object getObject(int columnIndex) throws SQLException {
	    return getObject(fieldNames[getFieldIndex(columnIndex)]);
	}

	public Object getObject(String columnLabel) throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("ResultSet.getItemByLabelFromClosedObject", Object.class.getName(), columnLabel));
		currentColumnLabel = columnLabel;
		try {
			ODatabaseRecordThreadLocal.INSTANCE.set(document.getDatabase());
	    	if (RID_COLUMN_NAME.equals(columnLabel)) {
	    		ORID rid = document.field(columnLabel, ORID.class);
	    		if (rid == null)
	    			return document.getIdentity();
	    		else
	    			return rid;
	    	}
			Object value = document.field(columnLabel);
			if (value == null)
				return null;
			else {
				// resolve the links so that the returned set contains instances
				// of ODocument
				if (value instanceof ORecordLazyMultiValue) {
					ORecordLazyMultiValue lazyRecord = (ORecordLazyMultiValue) value;
					lazyRecord.convertLinks2Records();
					return lazyRecord;
				} else return value;
			}
		} catch (Exception e) {
			throw new SQLException(ErrorMessages.get("ResultSet.getItem", columnLabel, Object.class.getName(), cursor + 1),e);
		}
	}

	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Ref getRef(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Ref getRef(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public int getRow() throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("ResultSet.getRowFromClosedObject"));
		return cursor + 1;
	}

	public RowId getRowId(int columnIndex) throws SQLException {
		return getRowId(fieldNames[getFieldIndex(columnIndex)]);
	}

	public RowId getRowId(String columnLabel) throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("ResultSet.getItemByLabelFromClosedObject", RowId.class.getName(), columnLabel));
		currentColumnLabel = columnLabel;
		try {
			ODatabaseRecordThreadLocal.INSTANCE.set(document.getDatabase());
    		ORID rid = document.field(columnLabel, ORID.class);
    		if (rid == null)
    			rid = document.getIdentity();
    		return new OrientRowId(rid);
		} catch (Exception e) {
			throw new SQLException(ErrorMessages.get("ResultSet.getItem", columnLabel, RowId.class.getName(), cursor + 1),e);
		}
	}

	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public short getShort(int columnIndex) throws SQLException {
		return getShort(fieldNames[getFieldIndex(columnIndex)]);
	}

	@SuppressWarnings("boxing")
	public short getShort(String columnLabel) throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("ResultSet.getItemByLabelFromClosedObject", Short.class.getName(), columnLabel));
		currentColumnLabel = columnLabel;
		try {
			ODatabaseRecordThreadLocal.INSTANCE.set(document.getDatabase());
			return (Short) document.field(columnLabel, OType.SHORT);
		} catch (Exception e) {
			throw new SQLException(ErrorMessages.get("ResultSet.getItem", columnLabel, Short.class.getName(), cursor + 1),e);
		}
	}

	public String getString(int columnIndex) throws SQLException {
		return getString(fieldNames[getFieldIndex(columnIndex)]);
	}

	public String getString(String columnLabel) throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("ResultSet.getItemByLabelFromClosedObject", String.class.getName(), columnLabel));
		currentColumnLabel = columnLabel;
		try {
			ODatabaseRecordThreadLocal.INSTANCE.set(document.getDatabase());
			return document.field(columnLabel, OType.STRING);
		} catch (Exception e) {
			throw new SQLException(ErrorMessages.get("ResultSet.getItem", columnLabel, String.class.getName(), cursor + 1),e);
		}
	}

	public Time getTime(int columnIndex) throws SQLException {
		return getTime(fieldNames[getFieldIndex(columnIndex)]);
	}

	public Time getTime(String columnLabel) throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("ResultSet.getItemByLabelFromClosedObject", String.class.getName(), columnLabel));
		currentColumnLabel = columnLabel;
		try {
			ODatabaseRecordThreadLocal.INSTANCE.set(document.getDatabase());
			java.util.Date dateTime = document.field(columnLabel, OType.DATETIME);
			if (dateTime == null)
				return null;
			return new Time(dateTime.getTime());
		} catch (Exception e) {
			throw new SQLException(ErrorMessages.get("ResultSet.getItem", columnLabel, String.class.getName(), cursor + 1),e);
		}
	}

	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		return new Time(getDate(columnIndex, cal).getTime());
	}

	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		return new Time(getDate(columnLabel, cal).getTime());
	}

	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return new Timestamp(getDate(columnIndex).getTime());
	}

	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		return new Timestamp(getDate(columnLabel).getTime());
	}

	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		return new Timestamp(getDate(columnIndex, cal).getTime());
	}

	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
		return new Timestamp(getDate(columnLabel, cal).getTime());
	}

	public int getType() throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("ResultSet.getTypeFromClosedObject", MESSAGE_FORMAT_CHOICE_INDEX));
		return type;
	}

	public URL getURL(int columnIndex) throws SQLException {
		return getURL(fieldNames[getFieldIndex(columnIndex)]);
	}

	public URL getURL(String columnLabel) throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("ResultSet.getItemByLabelFromClosedObject", String.class.getName(), columnLabel));
		currentColumnLabel = columnLabel;
		try {
			ODatabaseRecordThreadLocal.INSTANCE.set(document.getDatabase());
			String url = document.field(columnLabel, OType.STRING);
			if (url == null)
				return null;
			else
				return new URL(url);
		} catch (Exception e) {
			throw new SQLException(ErrorMessages.get("ResultSet.getItem", columnLabel, String.class.getName(), cursor + 1),e);
		}
	}

	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public SQLWarning getWarnings() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void insertRow() throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void moveToCurrentRow() throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void moveToInsertRow() throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public void refreshRow() throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("ResultSet.refreshRowOfClosedObject"));
	    if (type == FETCH_FORWARD)
	        throw new SQLException(ErrorMessages.get("ResultSet.refreshRowWhenForwardOnly", cursor + 1));
	    if (cursor == BEFORE_FIRST_INDEX || cursor == rowCount)
	        throw new SQLException(ErrorMessages.get("ResultSet.refreshRowOutOfBound", cursor));
	    try {
	        document.reload();
	    } catch (ORecordNotFoundException rnfe) {
	        throw new SQLException(ErrorMessages.get("ResultSet.refreshMissingRow", cursor + 1));
	    } catch (Exception e) {
	        throw new SQLException(ErrorMessages.get("ResultSet.refreshRow", cursor + 1));
	    }
	}

	public boolean rowDeleted() throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("ResultSet.getRowDeletedFromClosedObject"));
	    //TODO to be modified if the CONCUR_UPDATABLE
	    return false;
	}

	public boolean rowInserted() throws SQLException {
	    if (closed)
            throw new SQLException(ErrorMessages.get("ResultSet.getRowInsertedFromClosedObject"));
        //TODO to be modified if the CONCUR_UPDATABLE
        return false;
	}

	public boolean rowUpdated() throws SQLException {
	    if (closed)
            throw new SQLException(ErrorMessages.get("ResultSet.getRowUpdatedFromClosedObject"));
        //TODO to be modified if the CONCUR_UPDATABLE
        return false;
	}

	public void setFetchDirection(int direction) throws SQLException {
        if (closed)
            throw new SQLException(ErrorMessages.get("ResultSet.setFetchDirectionOnClosedObject"));
        switch (direction) {
            case FETCH_REVERSE:
                if (type == FETCH_FORWARD)
                    throw new SQLException(ErrorMessages.get("ResultSet.incompatibleFetchDirection"));
            case FETCH_FORWARD:
                fetchDirection = direction;
                break;
            case FETCH_UNKNOWN:
                fetchDirection = DEFAULT_FETCH_DIRECTION;
                break;
            default:
                throw new SQLException(ErrorMessages.get(
                        "ResultSet.badFetchDirection", FETCH_REVERSE + ", " + FETCH_FORWARD + ", " + FETCH_UNKNOWN, direction));
        }
	}

	public void setFetchSize(int rows) throws SQLException {
	    if (closed)
	        throw new SQLException(ErrorMessages.get("ResultSet.setFetchSizeOnClosedObject", MESSAGE_FORMAT_CHOICE_INDEX));
	    if (rows < 0)
	        throw new SQLException(ErrorMessages.get("ResultSet.badFetchSize", rows));
	    fetchSize = rows;
	    //update the rowCount only if the given fetch size is not unlimited AND
	    //is less than the current row count
	    if (fetchSize > OrientJdbcConstants.UNLIMITED_FETCH_SIZE && fetchSize < rowCount)
	        rowCount = fetchSize;
	}

	public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
	}

	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
	    throw new SQLFeatureNotSupportedException();
	}

	public boolean wasNull() throws SQLException {
		if (closed)
			throw new SQLException(ErrorMessages.get("ResultSet.getWasNullFromClosedObject", currentColumnLabel));
		return document.field(currentColumnLabel) == null;
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return ODocument.class.isAssignableFrom(iface);
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		try {
			return iface.cast(document);
		} catch (ClassCastException e) {
			throw new SQLException(e);
		}
	}

	public void cancelRowUpdates() throws SQLException {
	}

	public void clearWarnings() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}
	
	protected String[] getColumnNames () {
	    return this.fieldNames;
	}
}
