package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ClientTest {
    /**
     * The server task service used by this class.
     */
    private static ExecutorCompletionService<Void> serverService = new ExecutorCompletionService<Void>(
                                                                         Executors
                                                                                 .newCachedThreadPool());
    /**
     * The client task service used by this class.
     */
    private static ExecutorCompletionService<Void> clientService = new ExecutorCompletionService<Void>(
                                                                         Executors
                                                                                 .newCachedThreadPool());

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testClient() throws IOException, InterruptedException,
            ExecutionException {
        serverService.submit(new Server());
        clientService.submit(new Client(InetAddress.getLocalHost()));
        final Future<Void> future = clientService.take();
        Assert.assertNotNull(future);
        Assert.assertTrue(future.isDone());
        Assert.assertFalse(future.isCancelled());
        Assert.assertNull(future.get());
    }
}
