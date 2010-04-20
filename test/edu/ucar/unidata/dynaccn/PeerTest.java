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
        system(new String[] { "mkdir", "-p", "/tmp/server/out/subdir",
                "/tmp/server/in" });
        system(new String[] { "mkdir", "-p", "/tmp/client/out/subdir",
                "/tmp/client/in" });
        system(new String[] { "mkdir", "-p", "/tmp/client/out",
                "/tmp/client/in" });
        system(new String[] { "sh", "-c",
                "date > /tmp/server/out/server-file-1" });
        system(new String[] { "sh", "-c",
                "date > /tmp/server/out/server-file-2" });
        system(new String[] { "sh", "-c",
                "date > /tmp/server/out/subdir/server-subfile" });
        system(new String[] { "sh", "-c",
                "date > /tmp/client/out/client-file-1" });
        system(new String[] { "sh", "-c",
                "date > /tmp/client/out/client-file-2" });
        system(new String[] { "sh", "-c",
                "date > /tmp/client/out/subdir/client-subfile" });
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testPeers() throws IOException, InterruptedException,
            ExecutionException {
        final Attribute attribute = new Attribute("name");
        Constraint constraint = attribute.notEqualTo("client-file-2");
        Filter filter = new Filter(new Constraint[] { constraint });
        Predicate predicate = new Predicate(new Filter[] { filter });
        final Future<Void> serverFuture = serverService.submit(new Server(
                "/tmp/server/out", "/tmp/server/in", predicate));

        constraint = attribute.notEqualTo("server-file-2");
        filter = new Filter(new Constraint[] { constraint });
        predicate = new Predicate(new Filter[] { filter });
        final Future<Void> clientFuture = clientService.submit(new Client(
                InetAddress.getLocalHost(), "/tmp/client/out",
                "/tmp/client/in", predicate));

        // clientFuture.get();
        Thread.sleep(2000);
        clientFuture.cancel(true);
        Assert.assertTrue(clientFuture.isDone());
        try {
            clientFuture.get();
        }
        catch (final CancellationException e) {
        }
        catch (final ExecutionException e) {
            throw e;
        }
        serverFuture.cancel(true);
        Assert.assertTrue(serverFuture.isDone());
        try {
            serverFuture.get();
        }
        catch (final CancellationException e) {
        }

        Assert.assertTrue(new File("/tmp/client/in/server-file-1").exists());
        Assert
                .assertTrue(new File("/tmp/client/in/server-file-1").length() > 0);
        Assert.assertFalse(new File("/tmp/client/in/server-file-2").exists());
        Assert.assertTrue(new File("/tmp/client/in/subdir/server-subfile")
                .exists());
        Assert.assertTrue(new File("/tmp/client/in/subdir/server-subfile")
                .length() > 0);
        Assert.assertTrue(new File("/tmp/server/in/client-file-1").exists());
        Assert
                .assertTrue(new File("/tmp/server/in/client-file-1").length() > 0);
        Assert.assertFalse(new File("/tmp/server/in/client-file-2").exists());
        Assert.assertTrue(new File("/tmp/server/in/subdir/client-subfile")
                .exists());
        Assert.assertTrue(new File("/tmp/server/in/subdir/client-subfile")
                .length() > 0);
    }

    @Test
    public void testTermination() throws IOException, InterruptedException,
            ExecutionException {
        final Attribute attribute = new Attribute("name");
        Constraint constraint = attribute.notEqualTo("client-file-2");
        Filter filter = new Filter(new Constraint[] { constraint });
        Predicate predicate = new Predicate(new Filter[] { filter });
        final Future<Void> serverFuture = serverService.submit(new Server(
                "/tmp/server/out", "/tmp/server/in", predicate));

        constraint = attribute.equalTo("server-file-2");
        filter = new Filter(new Constraint[] { constraint });
        predicate = new Predicate(new Filter[] { filter });
        final Future<Void> clientFuture = clientService.submit(new Client(
                InetAddress.getLocalHost(), "/tmp/client/out",
                "/tmp/client/in", predicate));

        clientFuture.get();
        Assert.assertTrue(clientFuture.isDone());
        Assert.assertFalse(clientFuture.isCancelled());
        serverFuture.cancel(true);
        Assert.assertTrue(serverFuture.isDone());
        try {
            serverFuture.get();
        }
        catch (final CancellationException e) {
        }

        Assert.assertFalse(new File("/tmp/client/in/server-file-1").exists());
        Assert.assertTrue(new File("/tmp/client/in/server-file-2").exists());
        Assert
                .assertTrue(new File("/tmp/client/in/server-file-2").length() > 0);
        Assert.assertFalse(new File("/tmp/client/in/subdir/server-subfile")
                .exists());
    }
}
