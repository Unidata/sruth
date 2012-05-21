package edu.ucar.unidata.sruth;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
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
    private static final Path          TESTDIR     = Paths.get(
                                                           System.getProperty("java.io.tmpdir"))
                                                           .resolve(
                                                                   MultiClientTest.class
                                                                           .getSimpleName());
    /**
     * The task execution services
     */
    private CancellingExecutor         executor;
    private CompletionService<Void>    voidCompleter;
    private CompletionService<Boolean> booleanCompleter;
    /**
     * The sleep interval, in milliseconds, to be used.
     */
    private static long                sleepAmount = 2000;

    private static void system(final String[] cmd) throws IOException,
            InterruptedException {
        final Process process = Runtime.getRuntime().exec(cmd, null, null);
        Assert.assertNotNull(process);
        final int status = process.waitFor();
        Assert.assertEquals(0, status);
    }

    private static void removeTestDirectory() throws IOException,
            InterruptedException {
        system(new String[] { "rm", "-rf", TESTDIR.toString() });
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        removeTestDirectory();
        final Path serverDir = TESTDIR.resolve("server");
        Files.createDirectories(serverDir.resolve("subdir"));
        system(new String[] { "sh", "-c",
                "date > " + serverDir.resolve("file-1") });
        system(new String[] { "sh", "-c",
                "date > " + serverDir.resolve("file-2") });
        system(new String[] { "sh", "-c",
                "date > " + serverDir.resolve("subdir").resolve("subfile") });
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
        executor = new CancellingExecutor(0, Integer.MAX_VALUE, 0,
                TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
        voidCompleter = new ExecutorCompletionService<Void>(executor);
        booleanCompleter = new ExecutorCompletionService<Boolean>(executor);
        voidCompleter.submit(Misc.newReportingTask(voidCompleter));
        booleanCompleter.submit(Misc.newReportingTask(booleanCompleter));
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdownNow();
        Thread.interrupted();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }

    @Test
    public void testParallelDelivery() throws IOException,
            InterruptedException, ExecutionException {
        System.out.println("Parallel Delivery Test:");
        final Path testDir = TESTDIR;
        final Path clientDir = testDir.resolve("client");
        final Path parallelDir = clientDir.resolve("parallel");
        final Path clientDir1 = parallelDir.resolve("1");
        final Path clientDir2 = parallelDir.resolve("2");

        /*
         * Create the source.
         */
        Archive archive = new Archive(TESTDIR.resolve("server"));
        final Server sourceServer = new SourceServer(new ClearingHouse(archive,
                Predicate.NOTHING));
        final Future<Void> sourceServerFuture = voidCompleter
                .submit(sourceServer);
        final InetSocketAddress sourceServerAddress = sourceServer
                .getSocketAddress();

        /*
         * Create the first client that gets data from the source.
         */
        archive = new Archive(clientDir1);
        final ClearingHouse clearingHouse_1 = new ClearingHouse(archive,
                Predicate.EVERYTHING);
        final Client client_1 = new Client(new InetSocketAddress(1),
                sourceServerAddress, Filter.EVERYTHING, clearingHouse_1);

        /*
         * Create the second client that gets data from the source.
         */
        archive = new Archive(clientDir2);
        final ClearingHouse clearingHouse_2 = new ClearingHouse(archive,
                Predicate.EVERYTHING);
        final Client client_2 = new Client(new InetSocketAddress(2),
                sourceServerAddress, Filter.EVERYTHING, clearingHouse_2);

        /*
         * Start the clients.
         */
        final Future<Boolean> client_1_Future = booleanCompleter
                .submit(client_1);
        final Future<Boolean> client_2_Future = booleanCompleter
                .submit(client_2);

        Thread.sleep(sleepAmount);

        client_1_Future.cancel(true);
        client_2_Future.cancel(true);
        sourceServerFuture.cancel(true);

        for (final Path path : new Path[] { clientDir1, clientDir2 }) {
            File file = path.resolve("file-1").toFile();
            Assert.assertTrue(file.exists());
            Assert.assertTrue(file.length() > 0);
            file = path.resolve("file-2").toFile();
            Assert.assertTrue(file.exists());
            Assert.assertTrue(file.length() > 0);
            file = path.resolve("subdir").resolve("subfile").toFile();
            Assert.assertTrue(file.exists());
            Assert.assertTrue(file.length() > 0);
        }
    }

    @Test
    public void testSequentialDelivery() throws IOException,
            InterruptedException, ExecutionException {
        System.out.println();
        System.out.println("Sequential Delivery Test:");
        final Path commonArchivePath = TESTDIR.resolve("client").resolve(
                "series");
        final Path archive1Path = commonArchivePath.resolve("1");
        final Path archive2Path = commonArchivePath.resolve("2");

        /*
         * Create the source server.
         */
        Archive archive = new Archive(TESTDIR.resolve("server"));
        Server server = new SourceServer(new ClearingHouse(archive,
                Predicate.NOTHING));
        final Future<Void> server_1_Future = voidCompleter.submit(server);
        final InetSocketAddress server_1_Address = server.getSocketAddress();

        /*
         * Create the client/server pair that gets data from the first server
         * and sends data to the second client.
         */
        archive = new Archive(archive1Path);
        ClearingHouse clearingHouse = new ClearingHouse(archive,
                Predicate.EVERYTHING);
        server = new SinkServer(clearingHouse, new InetSocketAddressSet());
        final Future<Void> server_2_Future = voidCompleter.submit(server);
        final InetSocketAddress server_2_Address = server.getSocketAddress();
        Client client = new Client(server_2_Address, server_1_Address,
                Filter.EVERYTHING, clearingHouse);
        final Future<Boolean> client_1_Future = booleanCompleter.submit(client);

        /*
         * Create the second client that receives data from the client/server
         * pair.
         */
        archive = new Archive(archive2Path);
        clearingHouse = new ClearingHouse(archive, Predicate.EVERYTHING);
        client = new Client(new InetSocketAddress(2), server_2_Address,
                Filter.EVERYTHING, clearingHouse);
        final Future<Boolean> client_2_Future = booleanCompleter.submit(client);

        Thread.sleep(sleepAmount);
        // Thread.sleep(Long.MAX_VALUE);

        client_1_Future.cancel(true);
        client_2_Future.cancel(true);
        server_1_Future.cancel(true);
        server_2_Future.cancel(true);

        for (final Path archivePath : new Path[] { archive1Path, archive2Path }) {
            File file;
            for (int i = 1; i <= 2; i++) {
                file = archivePath.resolve("file-" + i).toFile();
                Assert.assertTrue(file.exists());
                Assert.assertTrue(file.length() > 0);
            }
            file = archivePath.resolve("subdir").resolve("subfile").toFile();
            Assert.assertTrue(file.exists());
            Assert.assertTrue(file.length() > 0);
        }
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
        final Future<Void> sourceServerFuture = voidCompleter
                .submit(sourceServer);
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
        final Future<Void> sinkServerFuture1 = voidCompleter
                .submit(sinkServer1);
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
        final Future<Void> sinkServerFuture2 = voidCompleter
                .submit(sinkServer2);
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
        final Future<Boolean> peerClientFuture1 = booleanCompleter
                .submit(peerClient1);
        final Future<Boolean> peerClientFuture2 = booleanCompleter
                .submit(peerClient2);
        final Future<Boolean> sourceClientFuture1 = booleanCompleter
                .submit(sourceClient1);
        final Future<Boolean> sourceClientFuture2 = booleanCompleter
                .submit(sourceClient2);

        Thread.sleep(sleepAmount);
        // Thread.sleep(Long.MAX_VALUE);

        Assert.assertEquals(2, sourceServer.getServletCount());
        Assert.assertEquals(2, sourceClearingHouse.getPeerCount());
        Assert.assertEquals(1, sinkServer1.getServletCount());
        Assert.assertEquals(1, sinkServer2.getServletCount());
        Assert.assertEquals(3, clearingHouse1.getPeerCount());
        Assert.assertEquals(3, clearingHouse2.getPeerCount());

        peerClientFuture1.cancel(true);
        peerClientFuture2.cancel(true);
        sourceClientFuture1.cancel(true);
        sourceClientFuture2.cancel(true);
        sourceServerFuture.cancel(true);
        sinkServerFuture1.cancel(true);
        sinkServerFuture2.cancel(true);

        Assert.assertTrue(new File(TESTDIR + "/client/peer/1/file-1").exists());
        Assert.assertTrue(new File(TESTDIR + "/client/peer/1/file-1").length() > 0);
        Assert.assertTrue(new File(TESTDIR + "/client/peer/1/file-2").exists());
        Assert.assertTrue(new File(TESTDIR + "/client/peer/1/file-2").length() > 0);
        Assert.assertTrue(new File(TESTDIR + "/client/peer/1/subdir/subfile")
                .exists());
        Assert.assertTrue(new File(TESTDIR + "/client/peer/1/subdir/subfile")
                .length() > 0);

        Assert.assertTrue(new File(TESTDIR + "/client/peer/2/file-1").exists());
        Assert.assertTrue(new File(TESTDIR + "/client/peer/2/file-1").length() > 0);
        Assert.assertTrue(new File(TESTDIR + "/client/peer/2/file-2").exists());
        Assert.assertTrue(new File(TESTDIR + "/client/peer/2/file-2").length() > 0);
        Assert.assertTrue(new File(TESTDIR + "/client/peer/2/subdir/subfile")
                .exists());
        Assert.assertTrue(new File(TESTDIR + "/client/peer/2/subdir/subfile")
                .length() > 0);
    }
}
