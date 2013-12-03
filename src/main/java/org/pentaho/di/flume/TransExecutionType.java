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

/**
 * Enum that defines if an execution will happen in block or nonblocking mode.
 * <p/>
 * Created Date: 02/12/2013
 * Created By: André Simões (andre.simoes@xpand-it.com)
 */
public enum TransExecutionType {
    BLOCKING,
    NONBLOCKING;

    /**
     * Convert from string to the enum class value, null = BLOCKING
     *
     * @param executionType the type of the execution
     * @return the executionType enum value
     */
    public static TransExecutionType getExecutionType(String executionType) {
        TransExecutionType set = TransExecutionType.BLOCKING;

        if (executionType.equalsIgnoreCase("nonblocking")) {
            set = TransExecutionType.NONBLOCKING;
        }

        return set;
    }
}
