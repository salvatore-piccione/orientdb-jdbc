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
package com.orientechnologies.orient.jdbc.connection;

import java.sql.SQLException;
import java.util.Properties;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.jdbc.OrientJdbcConnection;

/**
 * @author Salvatore Piccione (TXT e-solutions SpA - salvatore.piccione AT network.txtgroup.com)
 *
 */
public class OrientObjectDatabaseConnection extends OrientJdbcConnection {
    
    private ODatabaseObjectTx database;
    
    private static final String EMPTY_STRING_ARGUMENT = "The %1$s cannot be an empty string";
    
    public OrientObjectDatabaseConnection (String iUrl, String iUser, String iPassword, Properties iInfo) 
            throws NullPointerException, IllegalArgumentException, SQLException{
        if (iUrl.isEmpty())
            throw new IllegalArgumentException(String.format(EMPTY_STRING_ARGUMENT, "URL of the object database"));
        if (iUser.isEmpty())
            throw new IllegalArgumentException(String.format(EMPTY_STRING_ARGUMENT, "username"));
        if (iPassword.isEmpty())
            throw new IllegalArgumentException(String.format(EMPTY_STRING_ARGUMENT, "password"));
        try {
            /* The user does not expect that the connection is pooled so it is better to
             * provide a not-pooled database instance
             * 
             * database = ODatabaseObjectPool.global().acquire(iUrl, iUser, iPassword);
             */
            database = new ODatabaseObjectTx(iUrl).open(iUser, iPassword);
        } catch (Throwable t) {
            throw new SQLException(t);
        }
    }

    /* (non-Javadoc)
     * @see com.orientechnologies.orient.jdbc.OrientJdbcConnection#isWrapperForImpl(java.lang.Class)
     */
    @Override
    protected boolean isWrapperForImpl(Class<?> iface) {
        return iface.isInstance(this.database);
    }

    /* (non-Javadoc)
     * @see com.orientechnologies.orient.jdbc.OrientJdbcConnection#unwrapImpl(java.lang.Class)
     */
    @Override
    protected <T> T unwrapImpl(Class<T> iface) throws SQLException {
        try {
            return iface.cast(this.database);
        } catch (ClassCastException e) {
            throw new SQLException(e);
        }
    }
    
    /* (non-Javadoc)
     * @see com.orientechnologies.orient.jdbc.OrientJdbcConnection#getLowLevelDatabase()
     */
    @Override
    protected ODatabaseDocumentTx getOrientDatabase() {
        //TODO The Object Database is not built upon that kind of OrientDatabase.
        //The lowest-level common superclass is ODatabaseSchemaAware that cannot be passed
        //to ODatabaseRecordThreadLocal.INSTANCE.set() which is widely used in the code
        //to solve multi-threading issues.
        return null;
    }

    /* (non-Javadoc)
     * @see com.orientechnologies.orient.jdbc.OrientJdbcConnection#configTransaction()
     */
    @Override
    protected void configTransaction() {
        this.database.begin(TXTYPE.OPTIMISTIC);
    }

    /* (non-Javadoc)
     * @see com.orientechnologies.orient.jdbc.OrientJdbcConnection#commitImpl()
     */
    @Override
    protected void commitImpl() {
        this.database.commit();
    }

    /* (non-Javadoc)
     * @see com.orientechnologies.orient.jdbc.OrientJdbcConnection#rollabackImpl()
     */
    @Override
    protected void rollabackImpl() {
        this.database.rollback();
    }

    /* (non-Javadoc)
     * @see com.orientechnologies.orient.jdbc.OrientJdbcConnection#closeImpl()
     */
    @Override
    protected void closeImpl() throws SQLException {
        this.database.close();
    }

}
