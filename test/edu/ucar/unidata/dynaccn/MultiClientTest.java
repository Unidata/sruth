package edu.ucar.unidata.dynaccn;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
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
    private static ExecutorCompletionService<Void> executorService = new ExecutorCompletionService<Void>(
                                                                           Executors
                                                                                   .newCachedThreadPool());

    private static void system(final String[] cmd) throws IOException,
            InterruptedException {
        final Process process = Runtime.getRuntime().exec(cmd, null, null);
        Assert.assertNotNull(process);
        final int status = process.waitFor();
        Assert.assertEquals(0, status);
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
            try {
                future.get();
            }
            catch (final CancellationException e) {
                throw new AssertionError(); // can't happen
            }
        }
    }

    @Test
    public void testParallelDelivery() throws IOException,
            InterruptedException, ExecutionException {
        System.out.println("Parallel Delivery Test:");
        system(new String[] { "mkdir", "-p", "/tmp/client/parallel/1" });
        system(new String[] { "mkdir", "-p", "/tmp/client/parallel/2" });
        /*
         * Create the source.
         */
        final ClearingHouse sourceClearingHouse = new ClearingHouse(new File(
                "/tmp/server"), Predicate.NOTHING);
        final Server sourceServer = new Server(sourceClearingHouse);
        final Future<Void> sourceServerFuture = start(sourceServer);
        final ServerInfo sourceServerInfo = sourceServer.getServerInfo();

        /*
         * Create the first client that gets data from the source.
         */
        final ClearingHouse clearingHouse_1 = new ClearingHouse(new File(
                "/tmp/client/parallel/1"), Predicate.EVERYTHING);
        final Client client_1 = new Client(sourceServerInfo, clearingHouse_1);

        /*
         * Create the second client that gets data from the source.
         */
        final ClearingHouse clearingHouse_2 = new ClearingHouse(new File(
                "/tmp/client/parallel/2"), Predicate.EVERYTHING);
        final Client client_2 = new Client(sourceServerInfo, clearingHouse_2);

        /*
         * Start the clients.
         */
        final Future<Void> client_1_Future = start(client_1);
        final Future<Void> client_2_Future = start(client_2);

        Thread.sleep(500);

        stop(client_1_Future);
        stop(client_2_Future);
        stop(sourceServerFuture);

        Assert.assertTrue(new File("/tmp/client/parallel/1/server-file-1")
                .exists());
        Assert.assertTrue(new File("/tmp/client/parallel/1/server-file-1")
                .length() > 0);
        Assert.assertTrue(new File("/tmp/client/parallel/1/server-file-2")
                .exists());
        Assert.assertTrue(new File("/tmp/client/parallel/1/server-file-2")
                .length() > 0);
        Assert.assertTrue(new File(
                "/tmp/client/parallel/1/subdir/server-subfile").exists());
        Assert.assertTrue(new File(
                "/tmp/client/parallel/1/subdir/server-subfile").length() > 0);

        Assert.assertTrue(new File("/tmp/client/parallel/2/server-file-1")
                .exists());
        Assert.assertTrue(new File("/tmp/client/parallel/2/server-file-1")
                .length() > 0);
        Assert.assertTrue(new File("/tmp/client/parallel/2/server-file-2")
                .exists());
        Assert.assertTrue(new File("/tmp/client/parallel/2/server-file-2")
                .length() > 0);
        Assert.assertTrue(new File(
                "/tmp/client/parallel/2/subdir/server-subfile").exists());
        Assert.assertTrue(new File(
                "/tmp/client/parallel/2/subdir/server-subfile").length() > 0);
    }

    @Test
    public void testSequentialDelivery() throws IOException,
            InterruptedException, ExecutionException {
        System.out.println();
        System.out.println("Sequential Delivery Test:");
        system(new String[] { "mkdir", "-p", "/tmp/client/series/1" });
        system(new String[] { "mkdir", "-p", "/tmp/client/series/2" });
        /*
         * Create server.
         */
        ClearingHouse clearingHouse = new ClearingHouse(
                new File("/tmp/server"), Predicate.NOTHING);
        Server server = new Server(clearingHouse);
        final Future<Void> server_1_Future = start(server);
        final ServerInfo server_1_Info = server.getServerInfo();

        /*
         * Create client/server pair that gets data from the first server and
         * serves data to the second client.
         */
        clearingHouse = new ClearingHouse(new File("/tmp/client/series/1"),
                Predicate.EVERYTHING);
        Client client = new Client(server_1_Info, clearingHouse);
        final Future<Void> client_1_Future = start(client);
        server = new Server(clearingHouse);
        final Future<Void> server_2_Future = start(server);
        final ServerInfo server_2_Info = server.getServerInfo();

        /*
         * Create second client that gets data from the second server.
         */
        clearingHouse = new ClearingHouse(new File("/tmp/client/series/2"),
                Predicate.EVERYTHING);
        client = new Client(server_2_Info, clearingHouse);
        final Future<Void> client_2_Future = start(client);

        Thread.sleep(1000);
        // Thread.sleep(Long.MAX_VALUE);

        stop(client_1_Future);
        stop(client_2_Future);
        stop(server_1_Future);
        stop(server_2_Future);

        Assert.assertTrue(new File("/tmp/client/series/1/server-file-1")
                .exists());
        Assert.assertTrue(new File("/tmp/client/series/1/server-file-1")
                .length() > 0);
        Assert.assertTrue(new File("/tmp/client/series/1/server-file-2")
                .exists());
        Assert.assertTrue(new File("/tmp/client/series/1/server-file-2")
                .length() > 0);
        Assert
                .assertTrue(new File(
                        "/tmp/client/series/1/subdir/server-subfile").exists());
        Assert
                .assertTrue(new File(
                        "/tmp/client/series/1/subdir/server-subfile").length() > 0);

        Assert.assertTrue(new File("/tmp/client/series/2/server-file-1")
                .exists());
        Assert.assertTrue(new File("/tmp/client/series/2/server-file-1")
                .length() > 0);
        Assert.assertTrue(new File("/tmp/client/series/2/server-file-2")
                .exists());
        Assert.assertTrue(new File("/tmp/client/series/2/server-file-2")
                .length() > 0);
        Assert
                .assertTrue(new File(
                        "/tmp/client/series/2/subdir/server-subfile").exists());
        Assert
                .assertTrue(new File(
                        "/tmp/client/series/2/subdir/server-subfile").length() > 0);
    }

    @Test
    public void testPeerDelivery() throws IOException, InterruptedException,
            ExecutionException {
        System.out.println();
        System.out.println("Peer Delivery Test:");
        system(new String[] { "mkdir", "-p", "/tmp/client/peer/1" });
        system(new String[] { "mkdir", "-p", "/tmp/client/peer/2" });
        /*
         * Create the source server.
         */
        final ClearingHouse sourceClearingHouse = new ClearingHouse(new File(
                "/tmp/server"), Predicate.NOTHING);
        Server server = new Server(sourceClearingHouse);
        final Future<Void> sourceServerFuture = start(server);
        final ServerInfo sourceServerInfo = server.getServerInfo();

        /*
         * Create the left server.
         */
        final ClearingHouse leftClearingHouse = new ClearingHouse(new File(
                "/tmp/client/peer/1"), Predicate.EVERYTHING);
        server = new Server(leftClearingHouse);
        final Future<Void> leftServerFuture = start(server);
        final ServerInfo leftServerInfo = server.getServerInfo();

        /*
         * Create the right server.
         */
        final ClearingHouse rightClearingHouse = new ClearingHouse(new File(
                "/tmp/client/peer/2"), Predicate.EVERYTHING);
        server = new Server(rightClearingHouse);
        final Future<Void> rightServerFuture = start(server);
        final ServerInfo rightServerInfo = server.getServerInfo();

        /*
         * Create the clients.
         */
        final Client leftSourceClient = new Client(sourceServerInfo,
                leftClearingHouse);
        final Client rightSourceClient = new Client(sourceServerInfo,
                rightClearingHouse);
        final Client leftPeerClient = new Client(rightServerInfo,
                leftClearingHouse);
        final Client rightPeerClient = new Client(leftServerInfo,
                rightClearingHouse);

        /*
         * Start the clients.
         */
        final Future<Void> leftPeerClientFuture = start(leftPeerClient);
        final Future<Void> rightPeerClientFuture = start(rightPeerClient);
        final Future<Void> leftClientFuture = start(leftSourceClient);
        final Future<Void> rightClientFuture = start(rightSourceClient);

        Thread.sleep(1000);
        // Thread.sleep(Long.MAX_VALUE);

        stop(leftPeerClientFuture);
        stop(rightPeerClientFuture);
        stop(leftClientFuture);
        stop(rightClientFuture);
        stop(sourceServerFuture);
        stop(leftServerFuture);
        stop(rightServerFuture);

        Assert
                .assertTrue(new File("/tmp/client/peer/1/server-file-1")
                        .exists());
        Assert
                .assertTrue(new File("/tmp/client/peer/1/server-file-1")
                        .length() > 0);
        Assert
                .assertTrue(new File("/tmp/client/peer/1/server-file-2")
                        .exists());
        Assert
                .assertTrue(new File("/tmp/client/peer/1/server-file-2")
                        .length() > 0);
        Assert.assertTrue(new File("/tmp/client/peer/1/subdir/server-subfile")
                .exists());
        Assert.assertTrue(new File("/tmp/client/peer/1/subdir/server-subfile")
                .length() > 0);

        Assert
                .assertTrue(new File("/tmp/client/peer/2/server-file-1")
                        .exists());
        Assert
                .assertTrue(new File("/tmp/client/peer/2/server-file-1")
                        .length() > 0);
        Assert
                .assertTrue(new File("/tmp/client/peer/2/server-file-2")
                        .exists());
        Assert
                .assertTrue(new File("/tmp/client/peer/2/server-file-2")
                        .length() > 0);
        Assert.assertTrue(new File("/tmp/client/peer/2/subdir/server-subfile")
                .exists());
        Assert.assertTrue(new File("/tmp/client/peer/2/subdir/server-subfile")
                .length() > 0);
    }

    @Test
    public void testNodeDelivery() throws IOException, InterruptedException,
            ExecutionException {
        System.out.println();
        System.out.println("Node Delivery Test:");
        system(new String[] { "mkdir", "-p", "/tmp/client/node/1" });
        system(new String[] { "mkdir", "-p", "/tmp/client/node/2" });
        /*
         * Create the source node.
         */
        final Node sourceNode = new Node(new File("/tmp/server"),
                Predicate.NOTHING);
        /*
         * Create the sink nodes.
         */
        final Node sinkNode1 = new Node(new File("/tmp/client/node/1"),
                Predicate.EVERYTHING);
        final Node sinkNode2 = new Node(new File("/tmp/client/node/2"),
                Predicate.EVERYTHING);
        /*
         * Construct the network topology by hooking the nodes up.
         */
        final ServerInfo sourceNodeInfo = sourceNode.getServerInfo();
        sinkNode1.add(sourceNodeInfo);
        sinkNode2.add(sourceNodeInfo);
        sinkNode1.add(sinkNode2.getServerInfo());
        sinkNode2.add(sinkNode1.getServerInfo());
        /*
         * Start the nodes.
         */
        final Future<Void> sourceNodeFuture = start(sourceNode);
        final Future<Void> sinkNode1Future = start(sinkNode1);
        final Future<Void> sinkNode2Future = start(sinkNode2);

        Thread.sleep(500);
        // Thread.sleep(Long.MAX_VALUE);

        /*
         * Stop the nodes.
         */
        stop(sinkNode1Future);
        stop(sinkNode2Future);
        stop(sourceNodeFuture);

        Assert
                .assertTrue(new File("/tmp/client/node/1/server-file-1")
                        .exists());
        Assert
                .assertTrue(new File("/tmp/client/node/1/server-file-1")
                        .length() > 0);
        Assert
                .assertTrue(new File("/tmp/client/node/1/server-file-2")
                        .exists());
        Assert
                .assertTrue(new File("/tmp/client/node/1/server-file-2")
                        .length() > 0);
        Assert.assertTrue(new File("/tmp/client/node/1/subdir/server-subfile")
                .exists());
        Assert.assertTrue(new File("/tmp/client/node/1/subdir/server-subfile")
                .length() > 0);

        Assert
                .assertTrue(new File("/tmp/client/node/2/server-file-1")
                        .exists());
        Assert
                .assertTrue(new File("/tmp/client/node/2/server-file-1")
                        .length() > 0);
        Assert
                .assertTrue(new File("/tmp/client/node/2/server-file-2")
                        .exists());
        Assert
                .assertTrue(new File("/tmp/client/node/2/server-file-2")
                        .length() > 0);
        Assert.assertTrue(new File("/tmp/client/node/2/subdir/server-subfile")
                .exists());
        Assert.assertTrue(new File("/tmp/client/node/2/subdir/server-subfile")
                .length() > 0);
    }
}
