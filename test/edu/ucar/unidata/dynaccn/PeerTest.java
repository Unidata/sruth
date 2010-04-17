package edu.ucar.unidata.dynaccn;

import java.io.File;
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
    public void testClient() throws IOException, InterruptedException,
            ExecutionException {
        serverService.submit(new Server("/tmp/server/out", "/tmp/server/in"));
        final Future<Void> future = clientService.submit(new Client(InetAddress
                .getLocalHost(), "/tmp/client/out", "/tmp/client/in"));
        Thread.sleep(2000);
        future.cancel(true);
        Assert.assertTrue(future.isDone());
        Assert.assertTrue(future.isCancelled());
        Assert.assertTrue(new File("/tmp/client/in/server-file-1").exists());
        Assert.assertTrue(new File("/tmp/client/in/server-file-2").exists());
        Assert.assertTrue(new File("/tmp/client/in/subdir/server-subfile")
                .exists());
        Assert.assertTrue(new File("/tmp/server/in/client-file-1").exists());
        Assert.assertTrue(new File("/tmp/server/in/client-file-2").exists());
        Assert.assertTrue(new File("/tmp/server/in/subdir/client-subfile")
                .exists());
    }
}
