package org.pentaho.di.flume;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.channel.PseudoTxnMemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.lifecycle.LifecycleState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PentahoKettleSinkTest {

    private static String SINK_TRANS_NAME = "/flume_log_sink.ktr";
    private PentahoKettleSink sink;

    @Before
    public void setUp() throws Exception {
        sink = new PentahoKettleSink();
    }

    @Test
    public void testLifecycle() throws InterruptedException, LifecycleException {
        Channel channel = new PseudoTxnMemoryChannel();
        Context context = new Context();
        context.put(PentahoKettleSink.SINK_TRANS_PATH, getClass().getResource(SINK_TRANS_NAME).toExternalForm());
        context.put(PentahoKettleSink.SINK_EXECUTION_TYPE, "blocking");
        context.put(PentahoKettleSink.SINK_INJECTOR_NAME, "inject event");

        Configurables.configure(channel, new Context());
        Configurables.configure(sink, context);

        sink.setChannel(channel);
        sink.start();
        Assert.assertTrue("Reached start or error",
                LifecycleController.waitForOneOf(sink, LifecycleState.START_OR_ERROR));
        Assert.assertEquals("Sink is started", LifecycleState.START, sink.getLifecycleState());

        sink.stop();
        Assert.assertTrue("Reached stop or error", LifecycleController.waitForOneOf(sink, LifecycleState.STOP_OR_ERROR));
        Assert.assertEquals("Sink is stopped", LifecycleState.STOP, sink.getLifecycleState());
    }

    /**
     * Lack of exception test.
     */
    @Test
    public void testBlockingSink() throws InterruptedException, LifecycleException, EventDeliveryException {

        Channel channel = new PseudoTxnMemoryChannel();
        Context context = new Context();
        context.put(PentahoKettleSink.SINK_TRANS_PATH, getClass().getResource(SINK_TRANS_NAME).toExternalForm());
        context.put(PentahoKettleSink.SINK_EXECUTION_TYPE, "blocking");
        context.put(PentahoKettleSink.SINK_INJECTOR_NAME, "inject event");

        Configurables.configure(channel, new Context());
        Configurables.configure(sink, context);

        sink.setChannel(channel);
        sink.start();

        for (int i = 0; i < 5; i++) {
            Event event = EventBuilder.withBody(("Test " + i).getBytes());

            channel.put(event);
            sink.process();
        }

        sink.stop();
    }

    /**
     * Lack of exception test.
     */
    @Test
    public void testNonBlockingSink() throws InterruptedException, LifecycleException, EventDeliveryException {

        Channel channel = new PseudoTxnMemoryChannel();
        Context context = new Context();
        context.put(PentahoKettleSink.SINK_TRANS_PATH, getClass().getResource(SINK_TRANS_NAME).toExternalForm());
        context.put(PentahoKettleSink.SINK_EXECUTION_TYPE, "nonblocking");
        context.put(PentahoKettleSink.SINK_INJECTOR_NAME, "inject event");

        Configurables.configure(channel, new Context());
        Configurables.configure(sink, context);

        sink.setChannel(channel);
        sink.start();

        for (int i = 0; i < 5; i++) {
            Event event = EventBuilder.withBody(("Test " + i).getBytes());

            channel.put(event);
            sink.process();
        }

        sink.stop();
    }
}
