package com.orientechnologies.orient.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class OrientJdbcResultSetTest extends OrientJdbcBaseTest {

    @Test
    public void shouldMapReturnTypes() throws Exception {

        assertFalse(conn.isClosed());

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT stringKey, intKey, text, length, date FROM Item");

        ResultSetMetaData metaData = rs.getMetaData();

        assertNotNull(metaData);
    }
    
    @Test
    public void shouldReturnColumnValues() throws Exception {
    	assertFalse(conn.isClosed());

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT FROM Item WHERE intKey > 10");
        
        assertEquals("Unexpected result set type",ResultSet.TYPE_FORWARD_ONLY,rs.getType());
        
        assertTrue("Missing first row",rs.first());
        
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        
        assertEquals("Unexpected number of columns", 10, columnCount);
        int rowIndex = 1;
        do {
    		System.out.println("ROW:: " + rowIndex);
        	for (int i = 1; i <= columnCount; i++) {
        		System.out.println(metaData.getColumnName(i) + " = " + rs.getObject(i));
        	}
    		System.out.println("--------------");
        	rowIndex++;
        } while (rs.next());
    }

}
