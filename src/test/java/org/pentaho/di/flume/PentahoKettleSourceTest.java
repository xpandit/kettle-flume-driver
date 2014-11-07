package org.pentaho.di.flume;

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PentahoKettleSourceTest {

    private PentahoKettleSource source;
    private Channel channel;

    @Before
    public void setUp() throws Exception {
        source = new PentahoKettleSource();
        channel = new MemoryChannel();

        Configurables.configure(channel, new Context());

        List<Channel> channels = new ArrayList<Channel>();
        channels.add(channel);

        ChannelSelector rcs = new ReplicatingChannelSelector();
        rcs.setChannels(channels);

        source.setChannelProcessor(new ChannelProcessor(rcs));
    }

    @Test
    public void testLifecycle() throws InterruptedException {

        Context context = new Context();
        context.put(PentahoKettleSource.SOURCE_TRANS_PATH, getClass().getResource("/flume_sequence_source.ktr")
                .toExternalForm());
        context.put(PentahoKettleSource.SOURCE_EXECUTION_TYPE, "nonblocking");
        context.put(PentahoKettleSource.SOURCE_OUTPUT_NAME, "output");

        Configurables.configure(source, context);

        source.start();
        Assert.assertTrue("Reached start or error",
                LifecycleController.waitForOneOf(source, LifecycleState.START_OR_ERROR));
        Assert.assertEquals("Source is started", LifecycleState.START, source.getLifecycleState());

        source.stop();
        Assert.assertTrue("Reached stop or error",
                LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR));
        Assert.assertEquals("Source is stopped", LifecycleState.STOP, source.getLifecycleState());
    }

}
