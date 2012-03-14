package edu.ucar.unidata.sruth;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultiClientTest {
    /**
     * The test directory.
     */
    private static final String       TESTDIR         = "/tmp/MultiClientTest";
    /**
     * The task completion service.
     */
    private static CancellingExecutor executorService = new CancellingExecutor(
                                                              0,
                                                              Integer.MAX_VALUE,
                                                              0,
                                                              TimeUnit.SECONDS,
                                                              new SynchronousQueue<Runnable>());
    /**
     * The sleep interval, in milliseconds, to be used.
     */
    private static long               sleepAmount     = 2000;

    private static void system(final String[] cmd) throws IOException,
            InterruptedException {
        final Process process = Runtime.getRuntime().exec(cmd, null, null);
        Assert.assertNotNull(process);
        final int status = process.waitFor();
        Assert.assertEquals(0, status);
    }

    private static void removeTestDirectory() throws IOException,
            InterruptedException {
        system(new String[] { "rm", "-rf", TESTDIR });
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        removeTestDirectory();
        Files.createDirectories(Paths.get(TESTDIR + "/server/subdir"));
        system(new String[] { "sh", "-c",
                "date > " + TESTDIR + "/server/server-file-1" });
        system(new String[] { "sh", "-c",
                "date > " + TESTDIR + "/server/server-file-2" });
        system(new String[] { "sh", "-c",
                "date > " + TESTDIR + "/server/subdir/server-subfile" });
        final String sleepAmountString = System
                .getProperty(MultiClientTest.class.getName() + ".sleepAmount");
        if (null != sleepAmountString) {
            sleepAmount = Long.valueOf(sleepAmountString);
        }
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        // removeTestDirectory();
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
    private static <T> Future<T> start(final Callable<T> task) {
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
    private static <T> void stop(final Future<T> future)
            throws InterruptedException, ExecutionException {
        if (!future.cancel(true)) {
            future.get();
        }
    }

    @Test
    public void testParallelDelivery() throws IOException,
            InterruptedException, ExecutionException {
        System.out.println("Parallel Delivery Test:");
        final Path testDir = Paths.get(TESTDIR);
        final Path clientDir = testDir.resolve("client");
        final Path parallelDir = clientDir.resolve("parallel");
        final Path testDir1 = parallelDir.resolve("1");
        final Path testDir2 = parallelDir.resolve("2");
        testDir1.toFile().mkdirs();
        testDir2.toFile().mkdirs();

        /*
         * Create the source.
         */
        Archive archive = new Archive(TESTDIR + "/server");
        final Server sourceServer = new SourceServer(new ClearingHouse(archive,
                Predicate.NOTHING));
        final Future<Void> sourceServerFuture = start(sourceServer);
        final InetSocketAddress sourceServerAddress = sourceServer
                .getSocketAddress();

        /*
         * Create the first client that gets data from the source.
         */
        archive = new Archive(testDir1);
        final ClearingHouse clearingHouse_1 = new ClearingHouse(archive,
                Predicate.EVERYTHING);
        final Client client_1 = new Client(sourceServerAddress,
                sourceServerAddress, Filter.EVERYTHING, clearingHouse_1);

        /*
         * Create the second client that gets data from the source.
         */
        archive = new Archive(testDir2);
        final ClearingHouse clearingHouse_2 = new ClearingHouse(archive,
                Predicate.EVERYTHING);
        final Client client_2 = new Client(new InetSocketAddress(0),
                sourceServerAddress, Filter.EVERYTHING, clearingHouse_2);

        /*
         * Start the clients.
         */
        final Future<Boolean> client_1_Future = start(client_1);
        final Future<Boolean> client_2_Future = start(client_2);

        Thread.sleep(sleepAmount);

        stop(client_1_Future);
        stop(client_2_Future);
        stop(sourceServerFuture);

        for (final Path path : new Path[] { testDir1, testDir2 }) {
            File file = path.resolve("server-file-1").toFile();
            Assert.assertTrue(file.exists());
            Assert.assertTrue(file.length() > 0);
            file = path.resolve("server-file-2").toFile();
            Assert.assertTrue(file.exists());
            Assert.assertTrue(file.length() > 0);
            file = path.resolve("subdir").resolve("server-subfile").toFile();
            Assert.assertTrue(file.exists());
            Assert.assertTrue(file.length() > 0);
        }
    }

    @Test
    public void testSequentialDelivery() throws IOException,
            InterruptedException, ExecutionException {
        System.out.println();
        System.out.println("Sequential Delivery Test:");
        system(new String[] { "mkdir", "-p", TESTDIR + "/client/series/1" });
        system(new String[] { "mkdir", "-p", TESTDIR + "/client/series/2" });

        /*
         * Create first server.
         */
        Archive archive = new Archive(TESTDIR + "/server");
        Server server = new SourceServer(new ClearingHouse(archive,
                Predicate.NOTHING));
        final Future<Void> server_1_Future = start(server);
        final InetSocketAddress server_1_Address = server.getSocketAddress();

        /*
         * Create client/server pair that gets data from the first server and
         * sends data to the second client.
         */
        archive = new Archive(TESTDIR + "/client/series/1");
        ClearingHouse clearingHouse = new ClearingHouse(archive,
                Predicate.EVERYTHING);
        server = new SinkServer(clearingHouse, new InetSocketAddressSet());
        final Future<Void> server_2_Future = start(server);
        final InetSocketAddress server_2_Address = server.getSocketAddress();
        Client client = new Client(server_2_Address, server_1_Address,
                Filter.EVERYTHING, clearingHouse);
        final Future<Boolean> client_1_Future = start(client);

        /*
         * Create second client that receives data from the second server.
         */
        archive = new Archive(TESTDIR + "/client/series/2");
        clearingHouse = new ClearingHouse(archive, Predicate.EVERYTHING);
        client = new Client(server_2_Address, server_2_Address,
                Filter.EVERYTHING, clearingHouse);
        final Future<Boolean> client_2_Future = start(client);

        Thread.sleep(sleepAmount);
        // Thread.sleep(Long.MAX_VALUE);

        stop(client_1_Future);
        stop(client_2_Future);
        stop(server_1_Future);
        stop(server_2_Future);

        Assert.assertTrue(new File(TESTDIR + "/client/series/1/server-file-1")
                .exists());
        Assert.assertTrue(new File(TESTDIR + "/client/series/1/server-file-1")
                .length() > 0);
        Assert.assertTrue(new File(TESTDIR + "/client/series/1/server-file-2")
                .exists());
        Assert.assertTrue(new File(TESTDIR + "/client/series/1/server-file-2")
                .length() > 0);
        Assert.assertTrue(new File(TESTDIR
                + "/client/series/1/subdir/server-subfile").exists());
        Assert.assertTrue(new File(TESTDIR
                + "/client/series/1/subdir/server-subfile").length() > 0);

        Assert.assertTrue(new File(TESTDIR + "/client/series/2/server-file-1")
                .exists());
        Assert.assertTrue(new File(TESTDIR + "/client/series/2/server-file-1")
                .length() > 0);
        Assert.assertTrue(new File(TESTDIR + "/client/series/2/server-file-2")
                .exists());
        Assert.assertTrue(new File(TESTDIR + "/client/series/2/server-file-2")
                .length() > 0);
        Assert.assertTrue(new File(TESTDIR
                + "/client/series/2/subdir/server-subfile").exists());
        Assert.assertTrue(new File(TESTDIR
                + "/client/series/2/subdir/server-subfile").length() > 0);
    }

    @Test
    public void testPeerDelivery() throws IOException, InterruptedException,
            ExecutionException {
        System.out.println();
        System.out.println("Peer Delivery Test:");
        system(new String[] { "mkdir", "-p", TESTDIR + "/client/peer/1" });
        system(new String[] { "mkdir", "-p", TESTDIR + "/client/peer/2" });

        /*
         * Create the source server.
         */
        final Archive archive = new Archive(TESTDIR + "/server");
        final ClearingHouse sourceClearingHouse = new ClearingHouse(archive,
                Predicate.NOTHING);
        final Server sourceServer = new SourceServer(sourceClearingHouse);
        final Future<Void> sourceServerFuture = start(sourceServer);
        final InetSocketAddress sourceServerAddress = sourceServer
                .getSocketAddress();

        /*
         * Create sink server 1.
         */
        final Archive archive1 = new Archive(TESTDIR + "/client/peer/1");
        final ClearingHouse clearingHouse1 = new ClearingHouse(archive1,
                Predicate.EVERYTHING);
        final Server sinkServer1 = new SinkServer(clearingHouse1,
                new InetSocketAddressSet());
        final Future<Void> sinkServerFuture1 = start(sinkServer1);
        final InetSocketAddress sinkServerAddress1 = sinkServer1
                .getSocketAddress();

        /*
         * Create sink server 2.
         */
        final Archive archive2 = new Archive(TESTDIR + "/client/peer/2");
        final ClearingHouse clearingHouse2 = new ClearingHouse(archive2,
                Predicate.EVERYTHING);
        final Server sinkServer2 = new SinkServer(clearingHouse2,
                new InetSocketAddressSet());
        final Future<Void> sinkServerFuture2 = start(sinkServer2);
        final InetSocketAddress sinkServerAddress2 = sinkServer2
                .getSocketAddress();

        /*
         * Create the sink clients.
         */
        final Client sourceClient1 = new Client(sinkServerAddress1,
                sourceServerAddress, Filter.EVERYTHING, clearingHouse1);
        final Client sourceClient2 = new Client(sinkServerAddress2,
                sourceServerAddress, Filter.EVERYTHING, clearingHouse2);
        final Client peerClient1 = new Client(sinkServerAddress2,
                sinkServerAddress2, Filter.EVERYTHING, clearingHouse1);
        final Client peerClient2 = new Client(sinkServerAddress1,
                sinkServerAddress1, Filter.EVERYTHING, clearingHouse2);

        /*
         * Start the sink clients.
         */
        final Future<Boolean> peerClientFuture1 = start(peerClient1);
        final Future<Boolean> peerClientFuture2 = start(peerClient2);
        final Future<Boolean> sourceClientFuture1 = start(sourceClient1);
        final Future<Boolean> sourceClientFuture2 = start(sourceClient2);

        Thread.sleep(sleepAmount);
        // Thread.sleep(Long.MAX_VALUE);

        Assert.assertEquals(2, sourceServer.getServletCount());
        Assert.assertEquals(2, sourceClearingHouse.getPeerCount());
        Assert.assertEquals(1, sinkServer1.getServletCount());
        Assert.assertEquals(1, sinkServer2.getServletCount());
        Assert.assertEquals(3, clearingHouse1.getPeerCount());
        Assert.assertEquals(3, clearingHouse2.getPeerCount());

        stop(peerClientFuture1);
        stop(peerClientFuture2);
        stop(sourceClientFuture1);
        stop(sourceClientFuture2);
        stop(sourceServerFuture);
        stop(sinkServerFuture1);
        stop(sinkServerFuture2);

        Assert.assertTrue(new File(TESTDIR + "/client/peer/1/server-file-1")
                .exists());
        Assert.assertTrue(new File(TESTDIR + "/client/peer/1/server-file-1")
                .length() > 0);
        Assert.assertTrue(new File(TESTDIR + "/client/peer/1/server-file-2")
                .exists());
        Assert.assertTrue(new File(TESTDIR + "/client/peer/1/server-file-2")
                .length() > 0);
        Assert.assertTrue(new File(TESTDIR
                + "/client/peer/1/subdir/server-subfile").exists());
        Assert.assertTrue(new File(TESTDIR
                + "/client/peer/1/subdir/server-subfile").length() > 0);

        Assert.assertTrue(new File(TESTDIR + "/client/peer/2/server-file-1")
                .exists());
        Assert.assertTrue(new File(TESTDIR + "/client/peer/2/server-file-1")
                .length() > 0);
        Assert.assertTrue(new File(TESTDIR + "/client/peer/2/server-file-2")
                .exists());
        Assert.assertTrue(new File(TESTDIR + "/client/peer/2/server-file-2")
                .length() > 0);
        Assert.assertTrue(new File(TESTDIR
                + "/client/peer/2/subdir/server-subfile").exists());
        Assert.assertTrue(new File(TESTDIR
                + "/client/peer/2/subdir/server-subfile").length() > 0);
    }
}
