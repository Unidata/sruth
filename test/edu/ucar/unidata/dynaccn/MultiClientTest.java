package edu.ucar.unidata.dynaccn;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.CancellationException;
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

public class MultiClientTest {
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
        system(new String[] { "rm", "-rf", "/tmp/server", "/tmp/client-1",
                "/tmp/client-2" });
        system(new String[] { "mkdir", "-p", "/tmp/server/subdir" });
        system(new String[] { "sh", "-c", "date > /tmp/server/server-file-1" });
        system(new String[] { "sh", "-c", "date > /tmp/server/server-file-2" });
        system(new String[] { "sh", "-c",
                "date > /tmp/server/subdir/server-subfile" });
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testMultipleClients() throws IOException, InterruptedException,
            ExecutionException {
        final Server server = new Server("/tmp/server");

        server.start();

        final Future<Void> client1Future = clientService.submit(new Client(
                InetAddress.getLocalHost(), "/tmp/client-1",
                Predicate.EVERYTHING));
        final Future<Void> client2Future = clientService.submit(new Client(
                InetAddress.getLocalHost(), "/tmp/client-2",
                Predicate.EVERYTHING));

        Thread.sleep(2000);

        client1Future.cancel(true);
        Assert.assertTrue(client1Future.isDone());
        try {
            client1Future.get();
        }
        catch (final CancellationException e) {
        }
        catch (final ExecutionException e) {
            throw e;
        }

        client2Future.cancel(true);
        Assert.assertTrue(client2Future.isDone());
        try {
            client2Future.get();
        }
        catch (final CancellationException e) {
        }
        catch (final ExecutionException e) {
            throw e;
        }

        server.stop();

        Assert.assertTrue(new File("/tmp/client-1/server-file-1").exists());
        Assert.assertTrue(new File("/tmp/client-1/server-file-1").length() > 0);
        Assert.assertTrue(new File("/tmp/client-1/server-file-2").exists());
        Assert.assertTrue(new File("/tmp/client-1/server-file-2").length() > 0);
        Assert.assertTrue(new File("/tmp/client-1/subdir/server-subfile")
                .exists());
        Assert.assertTrue(new File("/tmp/client-1/subdir/server-subfile")
                .length() > 0);

        Assert.assertTrue(new File("/tmp/client-2/server-file-1").exists());
        Assert.assertTrue(new File("/tmp/client-2/server-file-1").length() > 0);
        Assert.assertTrue(new File("/tmp/client-2/server-file-2").exists());
        Assert.assertTrue(new File("/tmp/client-2/server-file-2").length() > 0);
        Assert.assertTrue(new File("/tmp/client-2/subdir/server-subfile")
                .exists());
        Assert.assertTrue(new File("/tmp/client-2/subdir/server-subfile")
                .length() > 0);
    }
}
