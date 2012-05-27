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

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.jdbc.common.OrientJdbcConnectionProperties;
import com.orientechnologies.orient.jdbc.common.OrientJdbcConstants;
import com.orientechnologies.orient.jdbc.connection.OrientDocumentDatabaseConnection;
import com.orientechnologies.orient.jdbc.connection.OrientNativeGraphConnection;
import com.orientechnologies.orient.jdbc.connection.OrientObjectDatabaseConnection;
import com.orientechnologies.orient.jdbc.connection.OrientTinkerpopGraphConnection;

public class OrientJdbcDriver implements java.sql.Driver {

    public static final int MAJOR_VERSION = 1;
    public static final int MINOR_VERSION = 0;
    
    public OLogManager logger = OLogManager.instance();
    
	static {
		try {
			java.sql.DriverManager.registerDriver(new OrientJdbcDriver());
		} catch (SQLException e) {
			OLogManager.instance().error(null, LogMessages.get("Driver.registrationError", OrientJdbcConstants.DRIVER_NAME),e);
		}
	}

	public boolean acceptsURL(String url) throws SQLException {
		return url.startsWith(OrientJdbcConstants.URL_PREFIX);
	}
	
    public Connection connect(String url, Properties info) throws SQLException {
        if (url == null) {
	        logger.error(this,LogMessages.get("Connection.nullDdUrl"));
	        throw new SQLException (ErrorMessages.get("Connection.nullDdUrl"));
	    }
	    if (url.isEmpty()) {
	        logger.error(this,LogMessages.get("Connection.emptyDbUrl"));
	        throw new SQLException(ErrorMessages.get("Connection.emptyDbUrl"));
	    }
	    if (info == null) {
	        logger.error(this,LogMessages.get("Connection.nullProperties"));
	        throw new SQLException(ErrorMessages.get("Connection.nullProperties"));
	    }
	    if (url.startsWith(OrientJdbcConstants.URL_PREFIX)) {
	        String dbUrl = url.replace(OrientJdbcConstants.URL_PREFIX, "");
	        String username = info.getProperty(OrientJdbcConnectionProperties.Keys.USERNAME);
	        String password = info.getProperty(OrientJdbcConnectionProperties.Keys.PASSWORD);
	        String connectionType = info.getProperty(OrientJdbcConnectionProperties.Keys.CONNECTION_TYPE);
	        try {
    	        if (null == connectionType)
    	            //the default type of database is ODocument
    	            return new OrientDocumentDatabaseConnection(dbUrl, username, password, info);
    	        else if (OrientJdbcConnectionProperties.Values.DOCUMENT_DB.equals(connectionType))
    	            return new OrientDocumentDatabaseConnection(dbUrl, username, password, info);
    	        else if (OrientJdbcConnectionProperties.Values.NATIVE_GRAPH_DB.equals(connectionType))
    	            return new OrientNativeGraphConnection(dbUrl, username, password, info);
    	        else if (OrientJdbcConnectionProperties.Values.OBJECT_DB.equals(connectionType))
    	            return new OrientObjectDatabaseConnection(dbUrl, username, password, info);
    	        else if (OrientJdbcConnectionProperties.Values.TINKERPOP_GRAPH_DB.equals(connectionType))
    	            return new OrientTinkerpopGraphConnection(dbUrl, username, password, info);
    	        else {
    	            logger.error(this, LogMessages.get("Connection.wrongType", OrientJdbcConnectionProperties.Keys.CONNECTION_TYPE,
                        connectionType, OrientJdbcConnectionProperties.Values.DOCUMENT_DB + ", " + 
                            OrientJdbcConnectionProperties.Values.NATIVE_GRAPH_DB + /* ", " +
                            OrientJdbcConnectionProperties.Values.OBJECT_DB + */", or " + 
                            OrientJdbcConnectionProperties.Values.TINKERPOP_GRAPH_DB));
    	            throw new SQLException (ErrorMessages.get("Connection.wrongType", OrientJdbcConnectionProperties.Keys.CONNECTION_TYPE,
	                    connectionType, OrientJdbcConnectionProperties.Values.DOCUMENT_DB + ", " + 
        	                OrientJdbcConnectionProperties.Values.NATIVE_GRAPH_DB + /* ", " +
        	                OrientJdbcConnectionProperties.Values.OBJECT_DB + */", or " + 
        	                OrientJdbcConnectionProperties.Values.TINKERPOP_GRAPH_DB));
    	        }
	        } catch (Exception e){
	        	logger.error(this, LogMessages.get("Driver.errorInConnectionCreation"), e);
	            if (e instanceof SQLException)
	                throw (SQLException)e;
	            else
	                throw new SQLException(e);
	        }
	    } else
	        return null;
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[]{};
    }

    public boolean jdbcCompliant() {
        return false;
    }

    public int getMajorVersion() {
        return MAJOR_VERSION;
    }

    public int getMinorVersion() {
        return MINOR_VERSION;
    }

    public static String getVersion() {
        return OrientJdbcConstants.DRIVER_NAME + " v" + MAJOR_VERSION + "." + MINOR_VERSION;
    }
}
