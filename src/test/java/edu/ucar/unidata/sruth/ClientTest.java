package edu.ucar.unidata.sruth;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

/**
 * Tests the {@link Client} class.
 * 
 * @author Steven R. Emmerson
 */
public class ClientTest {
    /**
     * The test directory.
     */
    private static final String        TESTDIR = "/tmp/ClientTest";
    /**
     * The logging service.
     */
    private static final Logger        logger  = Util.getLogger();
    /**
     * The execution service.
     */
    private CancellingExecutor         executorService;
    private CompletionService<Void>    voidCompleter;
    private CompletionService<Boolean> booleanCompleter;

    private static void system(final String[] cmd) throws IOException,
            InterruptedException {
        final Process process = Runtime.getRuntime().exec(cmd, null, null);
        Assert.assertNotNull(process);
        Assert.assertEquals(0, process.waitFor());
    }

    private static void removeTestDirectory() throws IOException,
            InterruptedException {
        system(new String[] { "rm", "-rf", TESTDIR });
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        removeTestDirectory();
        system(new String[] { "mkdir", "-p", TESTDIR + "/localServer/subdir" });
        system(new String[] { "sh", "-c",
                "date > " + TESTDIR + "/localServer/localServer-file-1" });
        system(new String[] { "sh", "-c",
                "date > " + TESTDIR + "/localServer/localServer-file-2" });
        system(new String[] { "sh", "-c",
                "date > " + TESTDIR + "/localServer/subdir/localServer-subfile" });
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        // removeTestDirectory();
    }

    @Before
    public void setUp() throws Exception {
        executorService = new CancellingExecutor(0, Integer.MAX_VALUE, 0,
                TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
        voidCompleter = new ExecutorCompletionService<Void>(executorService);
        booleanCompleter = new ExecutorCompletionService<Boolean>(
                executorService);
        voidCompleter.submit(Misc.newReportingTask(voidCompleter));
        booleanCompleter.submit(Misc.newReportingTask(booleanCompleter));
    }

    @After
    public void tearDown() throws Exception {
        executorService.shutdownNow();
        Thread.interrupted();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }

    @Test
    public void testTermination() throws IOException, InterruptedException,
            ExecutionException, TimeoutException {
        logger.info("testTermination():");
        system(new String[] { "mkdir", "-p", TESTDIR + "/client/term" });

        Archive archive = new Archive(Paths.get(TESTDIR + "/localServer"));
        final Server server = new SourceServer(new ClearingHouse(archive,
                Predicate.NOTHING));
        voidCompleter.submit(server);
        final InetSocketAddress serverSocketAddress = server.getSocketAddress();

        final Filter filter = Filter.getInstance("localServer-file-2");
        final Predicate predicate = new Predicate().add(filter);
        archive = new Archive(Paths.get(TESTDIR + "/client/term"));
        final ClearingHouse clearingHouse = new ClearingHouse(archive,
                predicate);
        final Client client = new Client(serverSocketAddress,
                serverSocketAddress, filter, clearingHouse);
        final Future<Boolean> clientFuture = booleanCompleter.submit(client);

        clientFuture.get(10, TimeUnit.SECONDS);

        executorService.shutdownNow();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

        Assert.assertFalse(new File(TESTDIR + "/client/term/localServer-file-1")
                .exists());
        Assert.assertTrue(new File(TESTDIR + "/client/term/localServer-file-2")
                .exists());
        Assert.assertTrue(new File(TESTDIR + "/client/term/localServer-file-2")
                .length() > 0);
        Assert.assertFalse(new File(TESTDIR
                + "/client/term/subdir/localServer-subfile").exists());
    }

    @Test
    public void testNonTermination() throws IOException, InterruptedException,
            ExecutionException {
        logger.info("testNonTermination():");
        system(new String[] { "mkdir", "-p", TESTDIR + "/client/nonterm" });

        Archive archive = new Archive(TESTDIR + "/localServer");
        final Server server = new SourceServer(new ClearingHouse(archive,
                Predicate.NOTHING));
        voidCompleter.submit(server);
        final InetSocketAddress serverSocketAddress = server.getSocketAddress();

        archive = new Archive(TESTDIR + "/client/nonterm");
        final ClearingHouse clearingHouse = new ClearingHouse(archive,
                Predicate.EVERYTHING);
        final Client client = new Client(serverSocketAddress,
                serverSocketAddress, Filter.EVERYTHING, clearingHouse);
        booleanCompleter.submit(client);

        Thread.sleep(1000);
        // Thread.sleep(Long.MAX_VALUE);

        executorService.shutdownNow();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

        Assert.assertTrue(new File(TESTDIR
                + "/client/nonterm/localServer-file-1").exists());
        Assert.assertTrue(new File(TESTDIR
                + "/client/nonterm/localServer-file-1").length() > 0);
        Assert.assertTrue(new File(TESTDIR
                + "/client/nonterm/localServer-file-2").exists());
        Assert.assertTrue(new File(TESTDIR
                + "/client/nonterm/localServer-file-2").length() > 0);
        Assert.assertTrue(new File(TESTDIR
                + "/client/nonterm/subdir/localServer-subfile").exists());
        Assert.assertTrue(new File(TESTDIR
                + "/client/nonterm/subdir/localServer-subfile").length() > 0);
    }
}
