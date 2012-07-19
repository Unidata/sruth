/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

/**
 * Tests node-level data-delivery.
 * 
 * @author Steven R. Emmerson
 */
public class NodeTest {

    /**
     * The logging service.
     */
    private static final Logger logger     = Util.getLogger();

    /**
     * The test directory.
     */
    private static final Path   TESTDIR    = Paths.get(
                                                   System.getProperty("java.io.tmpdir"))
                                                   .resolve(
                                                           NodeTest.class
                                                                   .getSimpleName());
    /**
     * The source test directory.
     */
    private static final Path   SOURCE_DIR = TESTDIR.resolve("source");
    /**
     * The sink test directory.
     */
    private static final Path   SINK_DIR   = TESTDIR.resolve("sink");
    /**
     * Files.
     */
    private static final Path   FILE_1     = Paths.get("file-1");
    private static final Path   FILE_2     = Paths.get("file-2");
    private static final Path   SUBDIR     = Paths.get("subdir");
    private static final Path   SUBFILE    = SUBDIR.resolve("subfile");

    private static void removeTestDirectory() throws IOException,
            InterruptedException {
        assertEquals(0, Misc.system("rm", "-rf", TESTDIR.toString()));
    }

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        removeTestDirectory();
        assertEquals(0, Misc.system("mkdir", "-p", SOURCE_DIR.resolve(SUBDIR)
                .toString()));
        assertEquals(0,
                Misc.system("sh", "-c", "date > " + SOURCE_DIR.resolve(FILE_1)));
        assertEquals(0,
                Misc.system("sh", "-c", "date > " + SOURCE_DIR.resolve(FILE_2)));
        assertEquals(0, Misc.system("sh", "-c",
                "date > " + SOURCE_DIR.resolve(SUBFILE)));
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        removeTestDirectory();
    }

    /**
     * The executor service.
     */
    private static class LoggingCancellingExecutor<T> extends
            CancellingExecutor {
        LoggingCancellingExecutor(final int minThread, final int maxThread,
                final long keepAlive, final TimeUnit timeUnit,
                final BlockingQueue<Runnable> queue) {
            super(minThread, maxThread, keepAlive, timeUnit, queue);
        }

        @Override
        protected void afterExecute(final Runnable r, final Throwable t) {
            super.afterExecute(r, t);
            if (t != null) {
                logger.error("Task problem:", t);
            }
            else {
                if (!(r instanceof Future<?>)) {
                    logger.error("Assertion error: {}", r);
                }
                else {
                    try {
                        ((Future<?>) r).get();
                    }
                    catch (final CancellationException e) {
                        logger.debug("Task was cancelled: {}", r);
                    }
                    catch (final ExecutionException e) {
                        if (e.getCause() instanceof InterruptedException) {
                            logger.debug("Task was interrupted: {}", r);
                        }
                        else {
                            logger.error("Task error: " + r, e);
                        }
                    }
                    catch (final InterruptedException e) {
                        logger.error("Assertion error", r);
                        Thread.currentThread().interrupt(); // ignore/reset
                    }
                }
            }
        }
    }

    private final LoggingCancellingExecutor<Void> executorService = new LoggingCancellingExecutor<Void>(
                                                                          0,
                                                                          Integer.MAX_VALUE,
                                                                          0,
                                                                          TimeUnit.SECONDS,
                                                                          new SynchronousQueue<Runnable>()) {
                                                                  };

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testNodes() throws IOException, InterruptedException,
            ExecutionException {
        logger.info("testNodes():");
        assertEquals(0, Misc.system("mkdir", "-p", SINK_DIR.toString()));

        /*
         * Create and start the source-node.
         */
        final SourceNode sourceNode = new SourceNode(new Archive(SOURCE_DIR));
        executorService.submit(sourceNode);
        final InetSocketAddress serverSocketAddress = sourceNode
                .getServerSocketAddress();

        /*
         * Create and start the tracker.
         */
        final Tracker tracker = new Tracker(serverSocketAddress,
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        final InetSocketAddress trackerAddress = tracker.getServerAddress();
        executorService.submit(tracker);

        /*
         * Create and start the sink-node.
         */
        final SinkNode sinkNode = new SinkNode(new Archive(SINK_DIR),
                Predicate.EVERYTHING, trackerAddress);
        executorService.submit(sinkNode);

        // Thread.sleep(Long.MAX_VALUE);
        Thread.sleep(2000);

        assertEquals(0, sourceNode.getClientCount());
        assertEquals(1, sourceNode.getServletCount());

        assertEquals(0, sinkNode.getServletCount());
        assertEquals(1, sinkNode.getClientCount());

        executorService.shutdownNow();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

        /*
         * stop(sinkFuture); stop(trackerFuture); stop(sourceFuture);
         * sinkNode.awaitCompletion(); tracker.awaitCompletion();
         * sourceNode.awaitCompletion();
         */

        File file;

        file = SINK_DIR.resolve(FILE_1).toFile();
        assertTrue(file.exists());
        assertTrue(file.length() > 0);

        file = SINK_DIR.resolve(FILE_2).toFile();
        assertTrue(file.exists());
        assertTrue(file.length() > 0);

        file = SINK_DIR.resolve(SUBFILE).toFile();
        assertTrue(file.exists());
        assertTrue(file.length() > 0);
    }

    @Test
    public void testNodeDelivery() throws IOException, InterruptedException,
            ExecutionException, FileInfoMismatchException {
        System.out.println();
        System.out.println("SourceNode Delivery Test:");
        final Path SINK_DIR_1 = SINK_DIR.resolve("_1");
        final Path SINK_DIR_2 = SINK_DIR.resolve("_2");
        /*
         * Create and start the source node.
         */
        final Archive serverArchive = new Archive(SOURCE_DIR);
        final SourceNode sourceNode = new SourceNode(serverArchive);
        final InetSocketAddress serverSocketAddress = sourceNode
                .getServerSocketAddress();
        executorService.submit(sourceNode);
        /*
         * Create and start the tracker.
         */
        final Tracker tracker = new Tracker(serverSocketAddress,
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        final InetSocketAddress trackerAddress = tracker.getServerAddress();
        executorService.submit(tracker);
        tracker.waitUntilRunning();
        /*
         * Create and start the sink nodes.
         */
        final SinkNode sinkNode1 = new SinkNode(new Archive(SINK_DIR_1),
                Predicate.EVERYTHING, trackerAddress);
        executorService.submit(sinkNode1);
        sinkNode1.waitUntilRunning();
        final SinkNode sinkNode2 = new SinkNode(new Archive(SINK_DIR_2),
                Predicate.EVERYTHING, trackerAddress);
        executorService.submit(sinkNode2);
        sinkNode2.waitUntilRunning();

        Thread.sleep(1000);

        assertEquals(0, sourceNode.getClientCount());
        assertEquals(2, sourceNode.getServletCount());

        SinkNode minClientNode, maxClientNode;
        if (sinkNode1.getClientCount() == 1) {
            minClientNode = sinkNode1;
            maxClientNode = sinkNode2;
        }
        else {
            minClientNode = sinkNode2;
            maxClientNode = sinkNode1;
        }
        assertEquals(1, minClientNode.getClientCount());
        assertEquals(2, maxClientNode.getClientCount());
        assertEquals(1, minClientNode.getServletCount());
        assertEquals(0, maxClientNode.getServletCount());

        /*
         * Test dropping a large file into the source directory.
         */
        final Path largeFilePath = SUBDIR.resolve("largeFile");
        final ByteBuffer byteBuf = ByteBuffer.wrap(new byte[1000000]);
        serverArchive.save(new ArchivePath(largeFilePath), byteBuf);

        Thread.sleep(2000);

        executorService.shutdownNow();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

        for (final Path sinkDir : new Path[] { SINK_DIR_1, SINK_DIR_2 }) {
            for (final Path name : new Path[] { FILE_1, FILE_2, SUBFILE }) {
                final File file = sinkDir.resolve(name).toFile();
                assertTrue(file.exists());
                assertTrue(file.length() > 0);
            }
        }

        for (final Path sinkDir : new Path[] { SINK_DIR_1, SINK_DIR_2 }) {
            final File file = sinkDir.resolve(largeFilePath).toFile();
            assertTrue(file.exists());
            assertTrue(file.length() == 1000000);
        }
    }

    // Obviated by addition of DelayedPathActionQueue to Archive
    // @Test
    public void testRemoval() throws IOException, InterruptedException,
            ExecutionException {
        System.out.println();
        System.out.println("Removal Test:");
        final Path REMOVAL_DIR_1 = SINK_DIR.resolve("removal").resolve("1");
        final Path REMOVAL_DIR_2 = SINK_DIR.resolve("removal").resolve("2");
        Misc.system("mkdir", "-p", REMOVAL_DIR_1.toString());
        Misc.system("mkdir", "-p", REMOVAL_DIR_2.toString());
        /*
         * Create and start the source node.
         */
        final Archive serverArchive = new Archive(SOURCE_DIR);
        final AbstractNode sourceNode = new SourceNode(serverArchive);
        final InetSocketAddress serverSocketAddress = sourceNode
                .getServerSocketAddress();
        executorService.submit(sourceNode);
        /*
         * Create and start the tracker.
         */
        final Tracker tracker = new Tracker(serverSocketAddress,
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        final InetSocketAddress trackerAddress = tracker.getServerAddress();
        executorService.submit(tracker);
        /*
         * Create and start the sink nodes.
         */
        final AbstractNode sinkNode1 = new SinkNode(new Archive(REMOVAL_DIR_1),
                Predicate.EVERYTHING, trackerAddress);
        executorService.submit(sinkNode1);
        final AbstractNode sinkNode2 = new SinkNode(new Archive(REMOVAL_DIR_2),
                Predicate.EVERYTHING, trackerAddress);
        executorService.submit(sinkNode2);

        Thread.sleep(1000);

        /*
         * Remove a file and a directory.
         */
        Misc.system("rm", SOURCE_DIR.resolve(FILE_1).toString());
        Misc.system("rm", "-rf", SOURCE_DIR.resolve(SUBFILE).toString());

        Thread.sleep(1000);
        // Thread.sleep(Long.MAX_VALUE);

        executorService.shutdownNow();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

        /*
         * Verify removals.
         */
        for (final Path removalDir : new Path[] { REMOVAL_DIR_1, REMOVAL_DIR_2 }) {
            for (final Path name : new Path[] { FILE_1, SUBDIR,
                    Paths.get(".sruth").resolve(SUBDIR) }) {
                final File file = removalDir.resolve(name).toFile();
                assertFalse(file.exists());
            }
        }
    }
}
