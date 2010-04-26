package edu.ucar.unidata.dynaccn;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
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
     * The executor service.
     */
    private static ExecutorService executorService = Executors
                                                           .newCachedThreadPool();

    /**
     * Starts a task.
     */
    private static Future<Void> start(final Callable<Void> task) {
        return executorService.submit(task);
    }

    /**
     * Stops a task.
     * 
     * @param future
     *            The future of the task to stop.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws ExecutionException
     *             if the task terminated due to an error.
     */
    private static void stop(final Future<Void> future)
            throws InterruptedException, ExecutionException {
        if (!future.cancel(true)) {
            future.get();
        }
    }

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
        system(new String[] { "mkdir", "-p", "/tmp/server/subdir",
                "/tmp/client" });
        system(new String[] { "sh", "-c", "date > /tmp/server/server-file-1" });
        system(new String[] { "sh", "-c", "date > /tmp/server/server-file-2" });
        system(new String[] { "sh", "-c",
                "date > /tmp/server/subdir/server-subfile" });
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testTermination() throws IOException, InterruptedException,
            ExecutionException {
        final Server server = new Server("/tmp/server");
        final Future<Void> serverFuture = start(server);

        final Attribute attribute = new Attribute("name");
        final Constraint constraint = attribute.equalTo("server-file-2");
        final Filter filter = new Filter(new Constraint[] { constraint });
        final Predicate predicate = new Predicate(new Filter[] { filter });
        final Client client = new Client(InetAddress.getLocalHost(),
                "/tmp/client", predicate);
        final Future<Void> clientFuture = start(client);

        clientFuture.get();
        stop(serverFuture);

        Assert.assertFalse(new File("/tmp/client/server-file-1").exists());
        Assert.assertTrue(new File("/tmp/client/server-file-2").exists());
        Assert.assertTrue(new File("/tmp/client/server-file-2").length() > 0);
        Assert.assertFalse(new File("/tmp/client/subdir/server-subfile")
                .exists());
    }

    @Test
    public void testPeers() throws IOException, InterruptedException,
            ExecutionException {
        final Server server = new Server("/tmp/server");
        final Future<Void> serverFuture = start(server);

        final Attribute attribute = new Attribute("name");
        final Constraint constraint = attribute.notEqualTo("server-file-2");
        final Filter filter = new Filter(new Constraint[] { constraint });
        final Predicate predicate = new Predicate(new Filter[] { filter });
        final Client client = new Client(InetAddress.getLocalHost(),
                "/tmp/client", predicate);
        final Future<Void> clientFuture = start(client);

        Thread.sleep(2500);
        stop(clientFuture);
        stop(serverFuture);

        Assert.assertTrue(new File("/tmp/client/server-file-1").exists());
        Assert.assertTrue(new File("/tmp/client/server-file-1").length() > 0);
        Assert.assertFalse(new File("/tmp/client/server-file-2").exists());
        Assert.assertTrue(new File("/tmp/client/subdir/server-subfile")
                .exists());
        Assert.assertTrue(new File("/tmp/client/subdir/server-subfile")
                .length() > 0);
    }
}
