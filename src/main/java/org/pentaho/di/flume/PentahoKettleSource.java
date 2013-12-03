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

import com.google.common.base.Preconditions;
import org.apache.commons.lang.ObjectUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.Logger;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.SingleThreadedTransExecutor;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepInterface;

import java.util.HashMap;
import java.util.Map;

/**
 * This Flume source uses a kettle transformation to generate the events which then forward to the channel.
 * <p/>
 * Created Date: 02/12/2013
 * Created By: André Simões (andre.simoes@xpand-it.com)
 */
public class PentahoKettleSource extends AbstractSource implements EventDrivenSource, Configurable {

    // The logger
    private static final Logger logger = Logger.getLogger(PentahoKettleSource.class);

    // Constants for the operation
    private static final String OUTPUT_HEADERS_FIELD_NAME = "eventHeader";

    private static final String OUTPUT_BODY_FIELD_NAME = "eventBody";

    private static final String SOURCE_TRANS_PATH = "sourceTransPath";

    private static final String SOURCE_OUTPUT_NAME = "sourceOutputName";

    private static final String SOURCE_EXECUTION_TYPE = "sourceExecutionType";

    private static final String SOURCE_LOG_LEVEL = "sourceLogLevel";

    private static final String DEFAULT_SOURCE_LOG_LEVEL = "BASIC";

    // Path to find the PDI transformation
    private String sourceTransPath;

    // Name of the step used to inject events
    private String sourceOutputName;

    // The logging level for the PDI execution
    private String sourceLogLevel;

    // Blocking vs nonblocking execution
    private TransExecutionType sourceExecutionType;

    // Runtime LRT (Long Running Transformation) object
    private Trans sourceTrans;

    // Single Threaded Transformation executor
    private SingleThreadedTransExecutor singleThreadedTransExecutor;

    // Internal event count
    private long eventCount = 0;

    // Lazy env init
    private PentahoKettleEnvironment env = PentahoKettleEnvironment.INSTANCE;

    @Override
    public void configure(Context context) {
        this.sourceTransPath = context.getString(SOURCE_TRANS_PATH);
        Preconditions.checkNotNull(this.sourceTransPath, "Please configure the sourceTransPath variable.");

        this.sourceOutputName = context.getString(SOURCE_OUTPUT_NAME);
        Preconditions.checkNotNull(this.sourceOutputName, "Please configure the sourceOutputName variable.");

        this.sourceExecutionType = TransExecutionType.getExecutionType(context.getString(SOURCE_EXECUTION_TYPE));

        this.sourceLogLevel = (String) ObjectUtils.defaultIfNull(context.getString(SOURCE_LOG_LEVEL), DEFAULT_SOURCE_LOG_LEVEL);
    }

    @Override
    public synchronized void start() {
        final ChannelProcessor channel = getChannelProcessor();

        try {
            // Initialize the sink transformation
            TransMeta transMeta = new TransMeta(this.sourceTransPath, (Repository) null);

            // If running in blocking mode, make sure the transformation type is single threaded
            if (sourceExecutionType == TransExecutionType.BLOCKING) {
                transMeta.setTransformationType(TransMeta.TransformationType.SingleThreaded);
                transMeta.setUsingThreadPriorityManagment(false);
            }

            sourceTrans = new Trans(transMeta);
            sourceTrans.prepareExecution(null);
            sourceTrans.setLogLevel(LogLevel.valueOf(this.sourceLogLevel));

            logger.debug("Loaded source transformation from: " + this.sourceTransPath);

            // find the "output" step
            StepInterface outputStep = sourceTrans.getStepInterface(this.sourceOutputName, 0);

            Preconditions.checkNotNull(outputStep, "Couldn't find Output step with name: " + this.sourceOutputName);

            RowAdapter rowAdapter = new RowAdapter() {
                @Override
                public void rowWrittenEvent(RowMetaInterface rowMeta, Object[] row) throws KettleStepException {
                    final Map<String, String> headers = new HashMap<String, String>();

                    String eventBody = null;

                    try {
                        eventBody = rowMeta.getString(row, OUTPUT_BODY_FIELD_NAME, "");
                    } catch (KettleValueException e) {
                        e.printStackTrace();
                    }

                    Event event = EventBuilder.withBody(eventBody.getBytes(), headers);
                    channel.processEvent(event);
                    eventCount++;
                }
            };

            outputStep.addRowListener(rowAdapter);

            // Initialize the transformation and wait for rows to be injected
            sourceTrans.startThreads();

            if (sourceExecutionType == TransExecutionType.BLOCKING) {
                singleThreadedTransExecutor = new SingleThreadedTransExecutor(sourceTrans);
                singleThreadedTransExecutor.init();
            }
        } catch (KettleException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void stop() {
        try {
            if (sourceExecutionType == TransExecutionType.BLOCKING) {
                singleThreadedTransExecutor.dispose();
            } else {
                sourceTrans.stopAll();
                sourceTrans.waitUntilFinished();
            }
        } catch (KettleException e) {
            e.printStackTrace();
        }

        logger.info("Total events processed: " + eventCount);
    }
}
