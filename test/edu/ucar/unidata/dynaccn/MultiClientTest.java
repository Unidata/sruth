package edu.ucar.unidata.dynaccn;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Callable;
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
    /**
     * The sleep interval to be used.
     */
    private static long                            sleepAmount     = 1000;

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
        final String sleepAmountString = System
                .getProperty(MultiClientTest.class.getName() + ".sleepAmount");
        if (null != sleepAmountString) {
            sleepAmount = Long.valueOf(sleepAmountString);
        }
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        // system(new String[] { "rm", "-rf", "/tmp/server", "/tmp/client" });
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
    public void testParallelDelivery() throws IOException,
            InterruptedException, ExecutionException {
        System.out.println("Parallel Delivery Test:");
        system(new String[] { "mkdir", "-p", "/tmp/client/parallel/1" });
        system(new String[] { "mkdir", "-p", "/tmp/client/parallel/2" });
        /*
         * Create the source.
         */
        Archive archive = new Archive("/tmp/server");
        final Server sourceServer = new Server(new ClearingHouse(archive,
                Predicate.NOTHING));
        final Future<Void> sourceServerFuture = start(sourceServer);
        final ServerInfo sourceServerInfo = sourceServer.getServerInfo();

        /*
         * Create the first client that gets data from the source.
         */
        archive = new Archive("/tmp/client/parallel/1");
        final ClearingHouse clearingHouse_1 = new ClearingHouse(archive,
                Predicate.EVERYTHING);
        final Client client_1 = new Client(sourceServerInfo, clearingHouse_1);

        /*
         * Create the second client that gets data from the source.
         */
        archive = new Archive("/tmp/client/parallel/2");
        final ClearingHouse clearingHouse_2 = new ClearingHouse(archive,
                Predicate.EVERYTHING);
        final Client client_2 = new Client(sourceServerInfo, clearingHouse_2);

        /*
         * Start the clients.
         */
        final Future<Void> client_1_Future = start(client_1);
        final Future<Void> client_2_Future = start(client_2);

        Thread.sleep(sleepAmount);

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
         * Create first server.
         */
        Archive archive = new Archive("/tmp/server");
        Server server = new Server(
                new ClearingHouse(archive, Predicate.NOTHING));
        final Future<Void> server_1_Future = start(server);
        final ServerInfo server_1_Info = server.getServerInfo();

        /*
         * Create client/server pair that gets data from the first server and
         * sends data to the second client.
         */
        archive = new Archive("/tmp/client/series/1");
        ClearingHouse clearingHouse = new ClearingHouse(archive,
                Predicate.EVERYTHING);
        Client client = new Client(server_1_Info, clearingHouse);
        final Future<Void> client_1_Future = start(client);
        server = new Server(clearingHouse);
        final Future<Void> server_2_Future = start(server);
        final ServerInfo server_2_Info = server.getServerInfo();

        /*
         * Create second client that receives data from the second server.
         */
        archive = new Archive("/tmp/client/series/2");
        clearingHouse = new ClearingHouse(archive, Predicate.EVERYTHING);
        client = new Client(server_2_Info, clearingHouse);
        final Future<Void> client_2_Future = start(client);

        Thread.sleep(sleepAmount);
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
        final Archive archive = new Archive("/tmp/server");
        final ClearingHouse sourceClearingHouse = new ClearingHouse(archive,
                Predicate.NOTHING);
        final Server sourceServer = new Server(sourceClearingHouse);
        final Future<Void> sourceServerFuture = start(sourceServer);
        final ServerInfo sourceServerInfo = sourceServer.getServerInfo();

        /*
         * Create sink server 1.
         */
        final Archive archive1 = new Archive("/tmp/client/peer/1");
        final ClearingHouse clearingHouse1 = new ClearingHouse(archive1,
                Predicate.EVERYTHING);
        final Server server1 = new Server(clearingHouse1);
        final Future<Void> serverFuture1 = start(server1);
        final ServerInfo serverInfo1 = server1.getServerInfo();

        /*
         * Create sink server 2.
         */
        final Archive archive2 = new Archive("/tmp/client/peer/2");
        final ClearingHouse clearingHouse2 = new ClearingHouse(archive2,
                Predicate.EVERYTHING);
        final Server server2 = new Server(clearingHouse2);
        final Future<Void> serverFuture2 = start(server2);
        final ServerInfo serverInfo2 = server2.getServerInfo();

        /*
         * Create the sink clients.
         */
        final Client sourceClient1 = new Client(sourceServerInfo,
                clearingHouse1);
        final Client sourceClient2 = new Client(sourceServerInfo,
                clearingHouse2);
        final Client peerClient1 = new Client(serverInfo2, clearingHouse1);
        final Client peerClient2 = new Client(serverInfo1, clearingHouse2);

        /*
         * Start the sink clients.
         */
        final Future<Void> peerClientFuture1 = start(peerClient1);
        final Future<Void> peerClientFuture2 = start(peerClient2);
        final Future<Void> sourceClientFuture1 = start(sourceClient1);
        final Future<Void> sourceClientFuture2 = start(sourceClient2);

        Thread.sleep(sleepAmount);
        // Thread.sleep(Long.MAX_VALUE);

        Assert.assertEquals(2, sourceServer.getClientCount());
        Assert.assertEquals(2, sourceClearingHouse.getPeerCount());
        Assert.assertEquals(1, server1.getClientCount());
        Assert.assertEquals(1, server2.getClientCount());
        Assert.assertEquals(3, clearingHouse1.getPeerCount());
        Assert.assertEquals(3, clearingHouse2.getPeerCount());

        stop(peerClientFuture1);
        stop(peerClientFuture2);
        stop(sourceClientFuture1);
        stop(sourceClientFuture2);
        stop(sourceServerFuture);
        stop(serverFuture1);
        stop(serverFuture2);

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
        System.out.println("SourceNode Delivery Test:");
        system(new String[] { "mkdir", "-p", "/tmp/client/node/1" });
        system(new String[] { "mkdir", "-p", "/tmp/client/node/2" });
        /*
         * Create the source node.
         */
        final Archive serverArchive = new Archive("/tmp/server");
        final SourceNode sourceNode = new SourceNode(serverArchive,
                Predicate.NOTHING);
        /*
         * Create the sink nodes.
         */
        final SinkNode sinkNode1 = new SinkNode(new Archive(
                "/tmp/client/node/1"), Predicate.EVERYTHING);
        final SinkNode sinkNode2 = new SinkNode(new Archive(
                "/tmp/client/node/2"), Predicate.EVERYTHING);
        /*
         * Construct the network topology by hooking the nodes up.
         */
        final ServerInfo sourceNodeInfo = sourceNode.getServerInfo();
        sinkNode1.add(sourceNodeInfo);
        sinkNode2.add(sourceNodeInfo);
        sinkNode1.add(sinkNode2.getServerInfo());
        /*
         * Start the nodes.
         */
        final Future<Void> sourceNodeFuture = start(sourceNode);
        final Future<Void> sinkNode1Future = start(sinkNode1);
        final Future<Void> sinkNode2Future = start(sinkNode2);

        Thread.sleep(sleepAmount);
        Thread.sleep(sleepAmount);

        Assert.assertEquals(2, sourceNode.getClientCount());
        Assert.assertEquals(2, sinkNode1.getPeerCount());
        Assert.assertEquals(2, sinkNode2.getPeerCount());

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

        /*
         * Test dropping a large file into the source directory.
         */
        final Path path = serverArchive.getHiddenForm(Paths
                .get("/tmp/server/subdir/server-subfile-2"));
        Files.createDirectories(path.getParent());
        final SeekableByteChannel channel = path.newByteChannel(
                StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        channel.write(ByteBuffer.wrap(new byte[1000000]));
        channel.close();
        path.moveTo(serverArchive.getVisibleForm(path));

        Thread.sleep(sleepAmount);

        Assert
                .assertTrue(new File(
                        "/tmp/client/node/1/subdir/server-subfile-2").exists());
        Assert
                .assertTrue(new File(
                        "/tmp/client/node/1/subdir/server-subfile-2").length() == 1000000);
        Assert
                .assertTrue(new File(
                        "/tmp/client/node/2/subdir/server-subfile-2").exists());
        Assert
                .assertTrue(new File(
                        "/tmp/client/node/2/subdir/server-subfile-2").length() == 1000000);

        Thread.sleep(sleepAmount);
        // Thread.sleep(Long.MAX_VALUE);

        /*
         * Stop the nodes.
         */
        stop(sinkNode1Future);
        stop(sinkNode2Future);
        stop(sourceNodeFuture);
    }

    // Obviated by addition of FileDeleter to Archive
    // @Test
    public void testRemoval() throws IOException, InterruptedException,
            ExecutionException {
        System.out.println("Removal Test:");
        system(new String[] { "mkdir", "-p", "/tmp/client/removal/1" });
        system(new String[] { "mkdir", "-p", "/tmp/client/removal/2" });
        /*
         * Create the source node.
         */
        final Archive serverArchive = new Archive("/tmp/server");
        final SourceNode sourceNode = new SourceNode(serverArchive,
                Predicate.NOTHING);
        /*
         * Create the sink nodes.
         */
        final SinkNode sinkNode1 = new SinkNode(new Archive(
                "/tmp/client/removal/1"), Predicate.EVERYTHING);
        final SinkNode sinkNode2 = new SinkNode(new Archive(
                "/tmp/client/removal/2"), Predicate.EVERYTHING);
        /*
         * Construct the network topology by hooking the nodes up.
         */
        final ServerInfo sourceNodeInfo = sourceNode.getServerInfo();
        sinkNode1.add(sourceNodeInfo);
        sinkNode2.add(sourceNodeInfo);
        sinkNode1.add(sinkNode2.getServerInfo());
        /*
         * Start the nodes.
         */
        final Future<Void> sourceNodeFuture = start(sourceNode);
        final Future<Void> sinkNode1Future = start(sinkNode1);
        final Future<Void> sinkNode2Future = start(sinkNode2);

        Thread.sleep(sleepAmount);

        /*
         * Remove a file and a directory.
         */
        system(new String[] { "rm", "/tmp/server/server-file-1" });
        system(new String[] { "rm", "-rf", "/tmp/server/subdir" });

        Thread.sleep(sleepAmount);
        Thread.sleep(sleepAmount);
        Thread.sleep(sleepAmount);
        Thread.sleep(sleepAmount);
        // Thread.sleep(Long.MAX_VALUE);

        /*
         * Verify removals.
         */
        Assert.assertFalse(new File("/tmp/client/removal/1/server-file-1")
                .exists());
        Assert.assertFalse(new File("/tmp/client/removal/2/server-file-1")
                .exists());
        Assert.assertFalse(new File("/tmp/client/removal/1/subdir").exists());
        Assert.assertFalse(new File("/tmp/client/removal/2/subdir").exists());
        Assert.assertFalse(new File("/tmp/client/removal/1/.dynaccn/subdir")
                .exists());
        Assert.assertFalse(new File("/tmp/client/removal/2/.dynaccn/subdir")
                .exists());

        /*
         * Stop the nodes.
         */
        stop(sinkNode1Future);
        stop(sinkNode2Future);
        stop(sourceNodeFuture);
    }
}
