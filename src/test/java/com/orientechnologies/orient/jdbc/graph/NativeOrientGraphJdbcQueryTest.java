/*
 * Copyright 2011-2012 TXT e-solutions SpA
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
 * 
 * This work was performed within the IoT_at_Work Project
 * and partially funded by the European Commission's
 * 7th Framework Programme under the research area ICT-2009.1.3
 * Internet of Things and enterprise environments.
 *
 * Authors:
 *      Salvatore Piccione (TXT e-solutions SpA)
 *
 * Contributors:
 *        Domenico Rotondi (TXT e-solutions SpA)
 */
package com.orientechnologies.orient.jdbc.graph;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

import junit.framework.Assert;

import org.junit.Test;

/**
 * @author Salvatore Piccione (TXT e-solutions SpA - salvatore.piccione AT network.txtgroup.com)
 *
 */
public class NativeOrientGraphJdbcQueryTest extends NativeOrientGraphJdbcBaseTest {
	
	@Test
	public void testQuery () {
		String query = "select * from OGraphVertex where description like '%Production Cell%'";
		System.out.println("Executing: " + query);
		try {
			
			Statement statement = conn.createStatement(
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
			
			ResultSet result = statement.executeQuery(query);
			
			ResultSetMetaData resultMetaData = null;
			Object value = null;
			while (result.next()) {
				//check if the result is a vertex
				resultMetaData = result.getMetaData();
				for (int i = 1; i <= resultMetaData.getColumnCount(); i++) {
					value = result.getObject(i);
					
					if (value instanceof Collection<?>) {
						Collection<?> list = (Collection<?>) value;
						System.out.println("This is a collection (class = " + list.getClass().getName() + "):");
						for (Object obj: list)
							System.out.println("\t" + obj.toString());
						System.out.println("---- end collection");
					} else
						System.out.println(resultMetaData.getColumnName(i) + ": " +
							value + " (" + value.getClass().getName() + ")");
				}
				System.out.println("--------------------------------");
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
			Assert.fail("An error occured during the execution of the SQL Query.");
		}
	}

	@Test
	public void testQuery_binaryData () {
		String query = "select * from OGraphVertex where name like '%binary record%'";
		System.out.println("Executing: " + query);
		try {
			
			Statement statement = conn.createStatement(
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
			
			ResultSet result = statement.executeQuery(query);
			
			ResultSetMetaData resultMetaData = null;
			Object value = null;
			while (result.next()) {
				//check if the result is a vertex
				resultMetaData = result.getMetaData();
				for (int i = 1; i <= resultMetaData.getColumnCount(); i++) {
					value = result.getObject(i);
					
					if (value instanceof Collection<?>) {
						Collection<?> list = (Collection<?>) value;
						System.out.println("---- Collection");
						for (Object obj: list)
							System.out.println("\t" /*+ obj*/ + " class: " + obj.getClass().getName());
						System.out.println("---- end Collection");
					} else					
						System.out.println(resultMetaData.getColumnName(i) + ": " +
							/*value +*/ " (" + value.getClass().getName() + ")");
				}
				System.out.println("--------------------------------");
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
			Assert.fail("An error occured during the execution of the SQL Query.");
		}
	}
}
