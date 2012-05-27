/***************************************************************************
 * Copyright 2011 TXT e-solutions SpA
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

import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.orientechnologies.orient.jdbc.graph.OrientGraphJdbcBaseTest;

/**
 * @author Salvatore Piccione (TXT e-solutions SpA)
 *
 */
public class SpringDataSourceTest extends OrientGraphJdbcBaseTest{
	  
    @Test
    public void testRouting () throws InterruptedException {
        ApplicationContext context = new ClassPathXmlApplicationContext("data-source-context.xml");
        
        AsynchTester[] testers = new AsynchTester[]{new AsynchTester(new IoTatWorkThread(context)),new AsynchTester(new CorenetThread(context))};
        for(int i = 0;i < testers.length; i++) {
            testers[i].start();
        }

        for(AsynchTester tester : testers)
            tester.test();

    }
}
