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
package com.orientechnologies.orient.jdbc.spring;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import org.junit.Assert;
import org.springframework.context.ApplicationContext;

import com.orientechnologies.orient.jdbc.util.DataSourceContextHolder;
import com.orientechnologies.orient.jdbc.util.RoutingDataSource;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

/**
 * @author Salvatore Piccione (TXT e-solutions SpA - salvatore.piccione AT network.txtgroup.com)
 *
 */
public class CorenetThread implements Runnable {
    
    private Connection connection;
    
    private static final String ROOT_VERTEX_NAME = "Manas S.p.A.";
    
    private static final String NAME = "Corenet Partner Repo - ";
    
    private static final String SUPPLY_RELATION_EDGE = "hasSupplier";
    
    public CorenetThread (ApplicationContext context) {
        DataSourceContextHolder.setContext("corenet");
        RoutingDataSource ds = context.getBean(RoutingDataSource.class);
        try {
            this.connection = ds.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    public void run() {
        try {
            PreparedStatement queryPrepStatement = connection.prepareStatement("SELECT FROM OGraphVertex WHERE name = ?");
            queryPrepStatement.setString(1, ROOT_VERTEX_NAME);
            ResultSet rs = queryPrepStatement.executeQuery();
            
            Assert.assertTrue(NAME + "There is no vertex with name " + ROOT_VERTEX_NAME, rs.first());
            //the RID is always the first element in the RS
            Object vertexId = rs.getObject(1);
            OrientGraph g = connection.unwrap(OrientGraph.class);
            Vertex v = g.getVertex(vertexId);
            Assert.assertNotNull(NAME + "There is no vertex for rid ", v);
            
            Iterator<Edge> containmentRels = v.getEdges(Direction.OUT,SUPPLY_RELATION_EDGE).iterator();
            Assert.assertTrue(NAME + "There is no edge with label '" + SUPPLY_RELATION_EDGE + "' coming from " + v.toString(), containmentRels.hasNext());
            
            String prefix = NAME + "Values:: ";
            StringBuilder textToBePrinted = new StringBuilder (prefix);
            Edge currentRel;
            while (containmentRels.hasNext()) {
                currentRel = containmentRels.next();
                if (textToBePrinted.length() == prefix.length())
                    textToBePrinted.append(currentRel.getVertex(Direction.IN).getProperty("name").toString());
                else
                    textToBePrinted.append(", " + currentRel.getVertex(Direction.IN).getProperty("name").toString());
            }
            System.out.println(textToBePrinted);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }

    }

}
