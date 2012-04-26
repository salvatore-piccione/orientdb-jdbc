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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.jdbc.common.OrientJdbcConnectionProperties;
import com.orientechnologies.orient.jdbc.common.OrientJdbcConstants;

/**
 * @author Salvatore Piccione (TXT e-solutions SpA - salvatore.piccione AT network.txtgroup.com)
 */
public class OrientDataSource implements DataSource {

    static {
	    try {
            java.sql.DriverManager.registerDriver(new OrientJdbcDriver());
        } catch (SQLException e) {
            OLogManager.instance().error(null, LogMessages.get("Driver.registrationError", OrientJdbcConstants.DRIVER_NAME),e);
        }
	}

    private String url;
    private String username;
    private String password;
	protected Properties configuration;

    private PrintWriter logger;
    private int loginTimeout;

    public PrintWriter getLogWriter() throws SQLException {
        return logger;
    }

    public void setLogWriter(PrintWriter out) throws SQLException {
        this.logger = out;
    }

    public void setLoginTimeout(int seconds) throws SQLException {
        this.loginTimeout = seconds;
    }

    public int getLoginTimeout() throws SQLException {
        return loginTimeout;
    }
	
	public void setConfiguration(Properties configuration) {
        this.configuration = configuration;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Connection getConnection() throws SQLException {
        return this.getConnection(username, password);
    }

    public Connection getConnection(String username, String password) throws SQLException {
        if (configuration == null) {
	        configuration = new Properties();
	    }
        configuration.setProperty(OrientJdbcConnectionProperties.Keys.USERNAME, username);
        configuration.setProperty(OrientJdbcConnectionProperties.Keys.PASSWORD, password);
		return DriverManager.getConnection(url, configuration);
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
