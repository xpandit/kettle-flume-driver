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
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LogWriter;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.SingleThreadedTransExecutor;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepInterface;


/**
 * This Flume sink uses kettle to process the event, and it blocks the execution when forwarding the event,
 * this means if errors occur during execution the back-off will be successful but the trade-off here is a
 * possible slower execution time.
 * <p/>
 * Created Date: 11/07/2013
 * Created By: André Simões (andre.simoes@xpand-it.com)
 */
public class PentahoKettleSink extends AbstractSink implements Configurable {

    // The logger
    private static final Logger logger = Logger.getLogger(PentahoKettleSink.class);

    // Constants for the operation
    private static final String INJECTOR_HEADERS_FIELD_NAME = "eventHeaders";

    private static final String INJECTOR_BODY_FIELD_NAME = "eventBody";

    private static final String SINK_TRANS_PATH = "sinkTransPath";

    private static final String SINK_INJECTOR_NAME = "sinkInjectorName";

    private static final String SINK_EXECUTION_TYPE = "sinkExecutionType";

    private static final String SINK_LOG_LEVEL = "sinkLogLevel";

    private static final String DEFAULT_SINK_LOG_LEVEL = "BASIC";

    // Path to find the PDI transformation
    private String sinkTransPath;

    // Name of the step used to inject events
    private String sinkInjectorName;

    // Blocking vs nonblocking execution
    private TransExecutionType sinkExecutionType;

    // The logging level for the PDI execution
    private String sinkLogLevel;

    // Runtime LRT (Long Running Transformation) object
    private Trans sinkTrans;

    // Single Threaded Transformation executor
    private SingleThreadedTransExecutor singleThreadedTransExecutor;

    // Runtime row producer object used to inject rows in the LRT
    private RowProducer sinkRowProducer;

    // Runtime stream metadata signature
    private RowMetaInterface injectorRowMeta;

    // Internal event count
    private long eventCount = 0;

    // Lazy env init
    private PentahoKettleEnvironment env = PentahoKettleEnvironment.INSTANCE;

    @Override
    public void start() {
        try {
            // Initialize the sink transformation
            TransMeta transMeta = new TransMeta(this.sinkTransPath, (Repository) null);

            // If running in blocking mode, make sure the transformation type is single threaded
            if (sinkExecutionType == TransExecutionType.BLOCKING) {
                transMeta.setTransformationType(TransMeta.TransformationType.SingleThreaded);
                transMeta.setUsingThreadPriorityManagment(false);
            }

            sinkTrans = new Trans(transMeta);
            sinkTrans.prepareExecution(null);
            sinkTrans.setLogLevel(LogLevel.valueOf(this.sinkLogLevel));

            logger.debug("Loaded sink transformation from: " + this.sinkTransPath);

            // Find the injector step and set it to consume rows from the row producer
            StepInterface injector = sinkTrans.findRunThread(this.sinkInjectorName);

            Preconditions.checkNotNull(injector, "Couldn't find Injector step with name: " + this.sinkInjectorName);

            injectorRowMeta = transMeta.getStepFields(injector.getStepMeta());
            sinkRowProducer = sinkTrans.addRowProducer(injector.getStepname(), 0);

            // Initialize the transformation and wait for rows to be injected
            sinkTrans.startThreads();

            if (sinkExecutionType == TransExecutionType.BLOCKING) {
                singleThreadedTransExecutor = new SingleThreadedTransExecutor(sinkTrans);
                singleThreadedTransExecutor.init();
            }
        } catch (KettleException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {
        sinkRowProducer.finished();

        try {
            if (sinkExecutionType == TransExecutionType.BLOCKING) {
                singleThreadedTransExecutor.oneIteration();
                singleThreadedTransExecutor.dispose();
            } else {
                sinkTrans.waitUntilFinished();
            }
        } catch (KettleException e) {
            e.printStackTrace();
        }

        logger.info("Total events processed: " + eventCount);
    }

    @Override
    public void configure(Context context) {
        this.sinkTransPath = context.getString(SINK_TRANS_PATH);
        Preconditions.checkNotNull(this.sinkTransPath, "Please configure the sinkTransPath variable.");

        this.sinkInjectorName = context.getString(SINK_INJECTOR_NAME);
        Preconditions.checkNotNull(this.sinkInjectorName, "Please configure the sinkInjectorName variable.");

        this.sinkExecutionType = TransExecutionType.getExecutionType(context.getString(SINK_EXECUTION_TYPE));

        this.sinkLogLevel = (String) ObjectUtils.defaultIfNull(context.getString(SINK_LOG_LEVEL), DEFAULT_SINK_LOG_LEVEL);
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        // Start transaction
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();

        try {
            Event event = ch.take();

            if (event != null) {
                // Process the event data
                // TODO: check if there is a better way of capturing the body
                String eventHeaders = event.getHeaders().toString();
                String eventBody = new String(event.getBody());

                // Setup the row with the field metadata
                Object[] row = new Object[injectorRowMeta.getFieldNames().length];
                row[injectorRowMeta.indexOfValue(INJECTOR_HEADERS_FIELD_NAME)] = eventHeaders;
                row[injectorRowMeta.indexOfValue(INJECTOR_BODY_FIELD_NAME)] = eventBody;

                // Inject the row
                sinkRowProducer.putRow(injectorRowMeta, row);

                if (sinkExecutionType == TransExecutionType.BLOCKING) {
                    singleThreadedTransExecutor.oneIteration();
                }

                eventCount++;

                status = Status.READY;
            } else {
                // poll or erroneous event
                status = Status.BACKOFF;
            }

            txn.commit();
        } catch (Throwable t) {
            txn.rollback();

            // Log exception, handle individual exceptions as needed
            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error) t;
            }
        } finally {
            txn.close();
        }

        return status;
    }
}
