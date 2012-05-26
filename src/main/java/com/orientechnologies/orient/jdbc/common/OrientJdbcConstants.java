/***************************************************************************
 * Copyright 2012 TXT e-solutions SpA
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
 **************************************************************************/
package com.orientechnologies.orient.jdbc.common;

/**
 * @author Salvatore Piccione (TXT e-solutions SpA - salvatore.piccione AT network.txtgroup.com)
 *
 */
public class OrientJdbcConstants {
    
    public static final String URL_PREFIX = "jdbc:orient:";
    
    public static final int MIN_COLUMN_INDEX = 1;
    
    public static final String DATABASE_PRODUCT_NAME = "OrientDB";

    public static final String DRIVER_NAME = "OrientDB JDBC Driver";
    
    public static final int UNLIMITED_FETCH_SIZE = 0;
    
    //see http://docs.oracle.com/javase/6/docs/technotes/guides/jdbc/
    public static final int MAJOR_JDBC_VERSION = 4;
    public static final int MINOR_JDBC_VERSION = 0;
}
