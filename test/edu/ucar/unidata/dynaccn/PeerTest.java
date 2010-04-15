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

public class PeerTest {
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

    private static void system(final String[] cmd) throws IOException,
            InterruptedException {
        final Process process = Runtime.getRuntime().exec(cmd, null, null);
        Assert.assertNotNull(process);
        Assert.assertEquals(0, process.waitFor());
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        system(new String[] { "rm", "-rf", "/tmp/server", "/tmp/client" });
        system(new String[] { "mkdir", "-p", "/tmp/server/out",
                "/tmp/server/in" });
        system(new String[] { "mkdir", "-p", "/tmp/client/out",
                "/tmp/client/in" });
        system(new String[] { "sh", "-c", "date > /tmp/server/out/server-file" });
        system(new String[] { "sh", "-c", "date > /tmp/client/out/client-file" });
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testClient() throws IOException, InterruptedException,
            ExecutionException {
        serverService.submit(new Server("/tmp/server/out", "/tmp/server/in"));
        clientService.submit(new Client(InetAddress.getLocalHost(),
                "/tmp/client/out", "/tmp/client/in"));
        final Future<Void> future = clientService.take();
        Assert.assertNotNull(future);
        Assert.assertTrue(future.isDone());
        Assert.assertFalse(future.isCancelled());
        Assert.assertNull(future.get());
    }
}
