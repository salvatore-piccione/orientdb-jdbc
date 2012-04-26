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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;

/**
 * 
 * @author Salvatore Piccione (TXT e-solutions SpA - salvatore.piccione AT network.txtgroup.com)
 */
public class NativeOrientGraphJdbcCreationHelper {
    static final String ADMINISTRATOR_NAME = "root";
    
    static final String ADMINISTRATOR_PWD =
        /*OrientDB 1.0 snapshot*/
        "3D0C711F443AF57320C20547E8A55BC11A432B9B313D8DEFFA2C1B798698985D";
	static final String URL_DB = "remote:iot-at-work1:2425/test-native-graph-orient-jdbc";
	
	static final String USERNAME = "admin";
	
	static final String PASSWORD = "admin";

	static final String NAME = "name";

	static final String DESCRIPTION = "description";
	
	static final String CONTAINMENT_EDGE = "contains";
	
	static final String COMPONENT_EDGE = "has";

	private static final String BINARY_DATA = "binary_data";

    private static final String LABEL = "label";
		
	public static void createGraphDatabase () {
		//REGISTER THE ENGINE
		//Orient.instance().registerEngine(new OEngineRemote());
		OGraphDatabase graphDB = null;
		try {
			/**/
			OServerAdmin adminTool = null;
			//I have to use a OServerAdmin instance to check the existance of a server
			//because the method ODatabaseXXX.exists() is not supported for remote database
			//In order to use such class, you have to use the root account
			adminTool = new OServerAdmin(URL_DB).connect(ADMINISTRATOR_NAME,
					ADMINISTRATOR_PWD);
			System.out.println("Successful connection to graphDB1.");	
			if (adminTool.existsDatabase()) {
				System.out.println("The database already exists. It will be deleted and created again.");
				adminTool.dropDatabase();
			} 
			System.out.println("The database is going to be created.");
			adminTool.createDatabase("graph", "local");
			adminTool.close();
			
			graphDB = new OGraphDatabase (URL_DB);
			graphDB.open(USERNAME,PASSWORD);
            
            graphDB.begin(TXTYPE.OPTIMISTIC);
            ODocument root = graphDB.createVertex();
            root.field(NAME, "Plant", OType.STRING);
            root.field(DESCRIPTION, "This is the Plant", OType.STRING);
            
            
            ODocument cell = graphDB.createVertex();
            cell.field(NAME, "Cell 1", OType.STRING);
            cell.field(DESCRIPTION, "This is the Production Cell 1", OType.STRING);
            graphDB.createEdge(root, cell).field(LABEL, CONTAINMENT_EDGE).save();
            
            ODocument cellComponent = graphDB.createVertex();
            cellComponent.field(NAME, "Cell Element A1", OType.STRING);
            cellComponent.field(DESCRIPTION, "This is an element of the production cell 1", OType.STRING);
            graphDB.createEdge(cell, cellComponent).field(LABEL, COMPONENT_EDGE).save();
            
            cell.save();
            cellComponent.save();
            
            cell = graphDB.createVertex();
			cell.field(NAME, "Cell 2", OType.STRING);
			cell.field(DESCRIPTION, "This is the Production Cell 2", OType.STRING);
			graphDB.createEdge(root, cell).field(LABEL, CONTAINMENT_EDGE).save();
			
			cellComponent = graphDB.createVertex();
			cellComponent.field(NAME, "Cell Element B1", OType.STRING);
			cellComponent.field(DESCRIPTION, "This is an element of the production cell 2", OType.STRING);
			graphDB.createEdge(cell, cellComponent).field(LABEL, COMPONENT_EDGE).save();
			
			cell.save();
			cellComponent.save();
			
			URI fileURI = NativeOrientGraphJdbcCreationHelper.class.getClassLoader().getResource("file.pdf").toURI();
			
			ODocument binaryVertex = graphDB.createVertex();
			binaryVertex.field(NAME, "NoSQL Definition (single binary record)",OType.STRING);
			binaryVertex.field(BINARY_DATA, loadFile(graphDB, fileURI));
			binaryVertex.save();
			
			binaryVertex = graphDB.createVertex();
			binaryVertex.field(NAME, "NoSQL Definition (multiple binary record)",OType.STRING);
			binaryVertex.field(BINARY_DATA, loadFile(graphDB, fileURI, 50000));
			binaryVertex.save();
			
			graphDB.commit();
			graphDB.close();
			System.out.println("The database has been created");
		} catch (Exception e) {
			System.err.println("An error occured during the creation of the database " + 
				URL_DB + ": " + e.getMessage());
			e.printStackTrace();
			if (graphDB != null) {
				graphDB.rollback();
				graphDB.close();
			}
		}
	}
	
	/**
	 * Loads the file in a single instance of {@link ORecordBytes}
	 * 
	 * @param database
	 * @param fileURI
	 * @return
	 * @throws IOException 
	 */
	private static ORecordBytes loadFile (ODatabaseRecord database, URI fileURI) throws IOException {
		BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(new File (fileURI)));
		ORecordBytes record = new ORecordBytes(database);
		record.fromInputStream(inputStream);
		return record;
	}
	
	private static List<ORecordBytes> loadFile (ODatabaseRecord database, URI fileURI, int bufferSize) throws IOException {
		if (bufferSize > 0) {
			//declaring the file
			File binaryFile = new File (fileURI);
			//store the length of the file (in bytes)
			long binaryFileLength = binaryFile.length();
			//store the number of records
			int numberOfRecords = (int) (binaryFileLength / bufferSize);
			//store the remainder of the division above
			int remainder = (int) (binaryFileLength % bufferSize);
			//if the remainder is greater than zero, the number of records is incremented by one
			if (remainder > 0)
				numberOfRecords++;
			//declaring the chunks of binary data
			List<ORecordBytes> binaryChuncks = new ArrayList<ORecordBytes> (numberOfRecords);
			//defining file stream
			BufferedInputStream binaryStream = new BufferedInputStream(new FileInputStream(
				binaryFile));
			byte[] chunk;
			ORecordBytes recordChunck;
			for (int i = 0; i < numberOfRecords; i++) {
				if (i == numberOfRecords - 1)
					chunk = new byte[remainder];
				else
					chunk = new byte[bufferSize];
				//loading binary chunk
				binaryStream.read(chunk);
				//storing the binary chunk in the ORecordBytes
				recordChunck = new ORecordBytes(database,chunk);
				database.save(recordChunck);
				binaryChuncks.add(recordChunck);
			}
			return binaryChuncks;
		} else
			return Arrays.asList(loadFile(database,fileURI));
	}
}