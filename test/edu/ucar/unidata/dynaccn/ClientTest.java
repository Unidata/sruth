package edu.ucar.unidata.dynaccn;

import java.io.File;
import java.io.IOException;
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

    private static void system(final String[] cmd) throws IOException,
            InterruptedException {
        final Process process = Runtime.getRuntime().exec(cmd, null, null);
        Assert.assertNotNull(process);
        Assert.assertEquals(0, process.waitFor());
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        system(new String[] { "rm", "-rf", "/tmp/server", "/tmp/client" });
        system(new String[] { "mkdir", "-p", "/tmp/server/subdir" });
        system(new String[] { "sh", "-c", "date > /tmp/server/server-file-1" });
        system(new String[] { "sh", "-c", "date > /tmp/server/server-file-2" });
        system(new String[] { "sh", "-c",
                "date > /tmp/server/subdir/server-subfile" });
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        system(new String[] { "rm", "-rf", "/tmp/server", "/tmp/client" });
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

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

    @Test
    public void testTermination() throws IOException, InterruptedException,
            ExecutionException {
        system(new String[] { "mkdir", "-p", "/tmp/client/term" });

        ClearingHouse clearingHouse = new ClearingHouse(
                new File("/tmp/server"), Predicate.NOTHING);
        final Server server = new Server(clearingHouse);
        final Future<Void> serverFuture = start(server);
        final ServerInfo serverInfo = server.getServerInfo();

        final Attribute attribute = new Attribute("name");
        final Constraint constraint = attribute.equalTo("server-file-2");
        final Filter filter = new Filter(new Constraint[] { constraint });
        final Predicate predicate = new Predicate(new Filter[] { filter });
        clearingHouse = new ClearingHouse(new File("/tmp/client/term"),
                predicate);
        final Client client = new Client(serverInfo, clearingHouse);
        final Future<Void> clientFuture = start(client);

        clientFuture.get();
        stop(serverFuture);

        Assert.assertFalse(new File("/tmp/client/term/server-file-1").exists());
        Assert.assertTrue(new File("/tmp/client/term/server-file-2").exists());
        Assert
                .assertTrue(new File("/tmp/client/term/server-file-2").length() > 0);
        Assert.assertFalse(new File("/tmp/client/term/subdir/server-subfile")
                .exists());
    }

    @Test
    public void testNonTermination() throws IOException, InterruptedException,
            ExecutionException {
        system(new String[] { "mkdir", "-p", "/tmp/client/nonterm" });

        ClearingHouse clearingHouse = new ClearingHouse(
                new File("/tmp/server"), Predicate.NOTHING);
        final Server server = new Server(clearingHouse);
        final Future<Void> serverFuture = start(server);
        final ServerInfo serverInfo = server.getServerInfo();

        final Attribute attribute = new Attribute("name");
        final Constraint constraint = attribute.notEqualTo("server-file-2");
        final Filter filter = new Filter(new Constraint[] { constraint });
        final Predicate predicate = new Predicate(new Filter[] { filter });
        clearingHouse = new ClearingHouse(new File("/tmp/client/nonterm"),
                predicate);
        final Client client = new Client(serverInfo, clearingHouse);
        final Future<Void> clientFuture = start(client);

        Thread.sleep(200);
        stop(clientFuture);
        stop(serverFuture);

        Assert.assertTrue(new File("/tmp/client/nonterm/server-file-1")
                .exists());
        Assert.assertTrue(new File("/tmp/client/nonterm/server-file-1")
                .length() > 0);
        Assert.assertFalse(new File("/tmp/client/nonterm/server-file-2")
                .exists());
        Assert.assertTrue(new File("/tmp/client/nonterm/subdir/server-subfile")
                .exists());
        Assert.assertTrue(new File("/tmp/client/nonterm/subdir/server-subfile")
                .length() > 0);
    }

    @Test
    public void testNodes() throws IOException, InterruptedException,
            ExecutionException {
        system(new String[] { "mkdir", "-p", "/tmp/client/node" });

        final Node sourceNode = new Node(new File("/tmp/server"),
                Predicate.NOTHING);
        final Future<Void> sourceFuture = start(sourceNode);

        final Node sinkNode = new Node(new File("/tmp/client/node"),
                Predicate.EVERYTHING);
        sinkNode.add(sourceNode.getServerInfo());
        final Future<Void> sinkFuture = start(sinkNode);

        Thread.sleep(100);
        stop(sinkFuture);
        stop(sourceFuture);

        Assert.assertTrue(new File("/tmp/client/node/server-file-1").exists());
        Assert
                .assertTrue(new File("/tmp/client/node/server-file-1").length() > 0);
        Assert.assertTrue(new File("/tmp/client/node/server-file-2").exists());
        Assert
                .assertTrue(new File("/tmp/client/node/server-file-2").length() > 0);
        Assert.assertTrue(new File("/tmp/client/node/subdir/server-subfile")
                .exists());
        Assert.assertTrue(new File("/tmp/client/node/subdir/server-subfile")
                .length() > 0);
    }
}
