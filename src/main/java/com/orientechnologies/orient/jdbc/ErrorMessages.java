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
package com.orientechnologies.orient.jdbc;

import java.text.MessageFormat;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * @author Salvatore Piccione (TXT e-solutions SpA - salvatore.piccione AT network.txtgroup.com)
 *
 */
public class ErrorMessages {
    
    private static final ResourceBundle MESSAGES;
    
    static {
        MESSAGES = ResourceBundle.getBundle(ErrorMessages.class.getName());
    }

    public static String get (String messageKey, Object ... arguments) {
        try {
            if (messageKey == null) {
                throw new IllegalArgumentException("Message key cannot be null");
            }

            String message = MESSAGES.getString(messageKey);

            if (message == null) {
                return "Missing error message for key '" + messageKey + "'";
            } else if (arguments != null) {
                return MessageFormat.format(message, arguments);
            } else
                return message;
        } catch (MissingResourceException e) {
            return '!' + messageKey + '!';
        }
    }
}
