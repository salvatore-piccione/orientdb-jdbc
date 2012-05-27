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

import java.sql.SQLException;

/**
 * @author Salvatore Piccione (TXT e-solutions SpA - salvatore.piccione AT network.txtgroup.com)
 *
 */
public interface ConnectionSpecificMetaData {
	
	/**
	 * See: {@link java.sql.DatabaseMetaData#supportsTransactionIsolationLevel(int)}
	 */
	public boolean supportsTransactionIsolationLevel(int level) throws SQLException;
	
	/**
	 * See: {@link java.sql.DatabaseMetaData#supportsTransaction()}
	 */
	public boolean supportsTransactions() throws SQLException;
	
	/**
	 * See: {@link java.sql.DatabaseMetaData#supportsResultSetConcurrency(int, int)}
	 */
	public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException;
	
	/**
	 * See: {@link java.sql.DatabaseMetaData#supportsResultSetType(int)}
	 */
	public boolean supportsResultSetType(int type) throws SQLException;
	
	/**
	 * See: {@link java.sql.DatabaseMetaData#supportsResultSetHoldability(int)}
	 */
	public boolean supportsResultSetHoldability(int holdability) throws SQLException;
}
