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
import com.orientechnologies.orient.jdbc.OrientJdbcConnection;
import com.tinkerpop.blueprints.pgm.TransactionalGraph.Conclusion;
import com.tinkerpop.blueprints.pgm.impls.orientdb.OrientGraph;

/**
 * @author Salvatore Piccione (TXT e-solutions SpA - salvatore.piccione AT network.txtgroup.com)
 *
 */
public class OrientTinkerpopGraphConnection extends OrientJdbcConnection{
    
    private OrientGraph graphDatabase;
    
    private static final String EMPTY_STRING_ARGUMENT = "The %1$s cannot be an empty string";

    /**
     * @param iUrl
     * @param iInfo
     * @throws SQLException 
     * @throws NullPointerException TODO
     * @throws IllegalArgumentException TODO
     */
    public OrientTinkerpopGraphConnection(String iUrl, String iUser, String iPassword, Properties iInfo) 
            throws SQLException, NullPointerException, IllegalArgumentException {
        if (iUrl.isEmpty())
            throw new IllegalArgumentException(String.format(EMPTY_STRING_ARGUMENT, "URL of the object database"));
        if (iUser.isEmpty())
            throw new IllegalArgumentException(String.format(EMPTY_STRING_ARGUMENT, "username"));
        if (iPassword.isEmpty())
            throw new IllegalArgumentException(String.format(EMPTY_STRING_ARGUMENT, "password"));
        try {
            this.graphDatabase = new OrientGraph(iUrl,iUser,iPassword);
        } catch (Throwable t) {
            throw new SQLException(t);
        }
    }

    /* (non-Javadoc)
     * @see com.orientechnologies.orient.jdbc.OrientJdbcConnection#getLowLevelDatabase()
     */
    @Override
    protected ODatabaseDocumentTx getOrientDatabase() {
        return this.graphDatabase.getRawGraph();
    }
    
    /* (non-Javadoc)
     * @see com.orientechnologies.orient.jdbc.OrientJdbcConnection#isWrapperForImpl(java.lang.Class)
     */
    @Override
    protected boolean isWrapperForImpl(Class<?> iface) {
        return iface.isInstance(this.graphDatabase) ||
                iface.isInstance(this.graphDatabase.getRawGraph());
    }

    /* (non-Javadoc)
     * @see com.orientechnologies.orient.jdbc.OrientJdbcConnection#unwrapImpl(java.lang.Class)
     */
    @Override
    protected <T> T unwrapImpl(Class<T> iface) throws SQLException {
        try {
            return iface.cast(graphDatabase);
        } catch (ClassCastException e) {
            try {
                return iface.cast(graphDatabase.getRawGraph());
            } catch (ClassCastException e2) {
                throw new SQLException(e2);
            }
        }
    }

    /* (non-Javadoc)
     * @see com.orientechnologies.orient.jdbc.OrientJdbcConnection#commitImpl()
     */
    @Override
    protected void commitImpl() {
        this.graphDatabase.stopTransaction(Conclusion.SUCCESS);
    }

    /* (non-Javadoc)
     * @see com.orientechnologies.orient.jdbc.OrientJdbcConnection#configTransaction()
     */
    @Override
    protected void configTransaction() {
        this.graphDatabase.setMaxBufferSize(0);
        this.graphDatabase.startTransaction();
    }

    /* (non-Javadoc)
     * @see com.orientechnologies.orient.jdbc.OrientJdbcConnection#rollabackImpl()
     */
    @Override
    protected void rollabackImpl() {
        this.graphDatabase.stopTransaction(Conclusion.FAILURE);
    }

    /* (non-Javadoc)
     * @see com.orientechnologies.orient.jdbc.OrientJdbcConnection#closeImpl()
     */
    @Override
    protected void closeImpl() throws SQLException {
        this.graphDatabase.shutdown();
    }

}
