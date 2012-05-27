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
package com.orientechnologies.orient.jdbc.util;

import org.springframework.util.Assert;

/**
 * @author Salvatore Piccione (TXT e-solutions SpA)
 *
 */
public class DataSourceContextHolder {
	/**
	 * Holds the context data as a thread local string variable.
	 */
	private static final ThreadLocal<String> CONTEXT_HOLDER = new ThreadLocal<String> ();
	
	/**
	 * Returns the current data source context.
	 * 
	 * @return the current data source context.
	 */
	public static String getContext() {
		return CONTEXT_HOLDER.get();
	}
	
	/**
	 * Sets the current data source context.
	 * 
	 * @param lookupKey the current data source context.
	 */
	public static void setContext (String lookupKey) {
		Assert.notNull(lookupKey, "The lookup key of the data source context cannot be null.");
		CONTEXT_HOLDER.set(lookupKey);
	}
	
	/**
	 * Removes the current data source context.
	 */
	public static void clearContext () {
		CONTEXT_HOLDER.remove();
	}
	
	/**
	 * States whether the company name has been set.
	 * 
	 * @return {@code true} if the context data has been defined, {@code false} otherwise.
	 */
	public static boolean hasContext () {
		return CONTEXT_HOLDER.get() != null;
	}
}
