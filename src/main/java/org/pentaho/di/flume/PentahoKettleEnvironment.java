/*! ******************************************************************************
 *
 * Kettle Flume Driver
 *
 * Copyright (C) 2003-2013 by Xpand IT : http://www.xpand-it.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.flume;

import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogWriter;

/**
 * Utility enum for singleton initialisation of the Pentaho Data Integration environment
 * <p/>
 * Created Date: 02/12/2013
 * Created By: André Simões (andre.simoes@xpand-it.com)
 */
public enum PentahoKettleEnvironment {
    INSTANCE;

    private static final String FLUME_LOG4J_FILE_APPENDER = "LOGFILE";

    private PentahoKettleEnvironment() {
        try {
            KettleEnvironment.init();

            Logger rootLogger = Logger.getRootLogger();
            Appender flumeAppender = rootLogger.getAppender(FLUME_LOG4J_FILE_APPENDER);

            // Link the PDI logging to the flume logging if it is defined
            if (flumeAppender != null) {
                LogWriter.getInstance().addAppender(flumeAppender);
            }
        } catch (KettleException e) {
            e.printStackTrace();
        }
    }
}
