/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test of the {@link FileDeleter} class.
 * 
 * @author Steven R. Emmerson
 */
public class FileDeleterTest {
    /**
     * The logger for this class.
     */
    private static Logger          logger       = LoggerFactory
                                                        .getLogger(FileDeleterTest.class);
    /**
     * Pathname of the file-deletion test-directory.
     */
    private static Path            testDirPath  = Paths.get("/tmp/FileDeleter");
    /**
     * Pathname of the file-deletion queue.
     */
    private static Path            queuePath    = testDirPath.resolve("queue");
    /**
     * The executor service for the file-deletion tasks.
     */
    private static ExecutorService executor     = Executors
                                                        .newFixedThreadPool(1);
    /**
     * The file-deleter.
     */
    private FileDeleter            fileDeleter;
    /**
     * The file-deleter future.
     */
    private Future<Void>           fileDeleterFuture;
    /**
     * Pathname of the test-file.
     */
    private final Path             testFilePath = testDirPath.resolve("file");

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Misc.system("rm", "-rf", testDirPath.toString());
        testDirPath.createDirectory();
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Misc.system("rm", "-rf", testDirPath.toString());
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        queuePath.deleteIfExists();
        fileDeleter = new FileDeleter(testDirPath, new FileDeletionQueue(
                queuePath));
        fileDeleterFuture = executor.submit(new FutureTask<Void>(fileDeleter) {
            @Override
            protected void done() {
                if (!isCancelled()) {
                    try {
                        get();
                    }
                    catch (final InterruptedException ignored) {
                    }
                    catch (final ExecutionException e) {
                        final Throwable cause = e.getCause();
                        if (cause instanceof InterruptedException) {
                            // ignored
                        }
                        else {
                            logger.error("File-deleter terminated", cause);
                            System.exit(1);
                        }
                    }
                }
            }
        }, null);
        testFilePath.createFile();
        Assert.assertTrue(testFilePath.exists());
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        testFilePath.deleteIfExists();
        fileDeleterFuture.cancel(true);
        queuePath.deleteIfExists();
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.dynaccn.FileDeleter#FileDeleter(edu.ucar.unidata.dynaccn.FileDeletionQueue)}
     * .
     * 
     * @throws IOException
     */
    @Test
    public final void testFileDeleter() throws IOException {
        Assert.assertNotNull(fileDeleter);
        Assert.assertTrue(queuePath.exists());
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.dynaccn.FileDeleter#delete(java.nio.file.Path, long)}
     * .
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public final void testDeleteNoWait() throws IOException,
            InterruptedException {
        fileDeleter.delete(testFilePath, 0);
        fileDeleter.waitUntilEmpty();
        Assert.assertFalse(testFilePath.exists());
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.dynaccn.FileDeleter#delete(java.nio.file.Path, long)}
     * .
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public final void testDeleteWait() throws IOException, InterruptedException {
        fileDeleter.delete(testFilePath, 1);
        fileDeleter.waitUntilEmpty();
        Assert.assertFalse(testFilePath.exists());
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.dynaccn.FileDeleter#getPendingCount()}.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public final void testGetPendingCount() throws IOException,
            InterruptedException {
        Assert.assertEquals(0, fileDeleter.getPendingCount());
        fileDeleter.delete(testFilePath, 1);
        Assert.assertEquals(1, fileDeleter.getPendingCount());
        fileDeleter.waitUntilEmpty();
        Assert.assertEquals(0, fileDeleter.getPendingCount());
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.dynaccn.FileDeleter#getDeletedCount()}.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public final void testGetDeletedCount() throws IOException,
            InterruptedException {
        Assert.assertEquals(0, fileDeleter.getDeletedCount());
        fileDeleter.delete(testFilePath, 1);
        fileDeleter.waitUntilEmpty();
        Assert.assertEquals(1, fileDeleter.getDeletedCount());
    }

    /**
     * Test performance.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public final void testPerformance() throws IOException,
            InterruptedException {
        final int PERFORMANCE_FILE_COUNT = 1000;
        final Long startTime = System.currentTimeMillis();
        for (int i = 0; i < PERFORMANCE_FILE_COUNT; ++i) {
            fileDeleter.delete(createFile(i), 1);
        }
        fileDeleter.waitUntilEmpty();
        Assert.assertEquals(0, fileDeleter.getPendingCount());
        Assert.assertEquals(PERFORMANCE_FILE_COUNT, fileDeleter
                .getDeletedCount());
        final Long stopTime = System.currentTimeMillis();
        System.out.println("Number of files: " + PERFORMANCE_FILE_COUNT);
        System.out.println("Deletion rate: " + PERFORMANCE_FILE_COUNT
                / (double) (stopTime - startTime) * 1000 + "/s");
    }

    private Path createFile(final int i) throws IOException {
        final Path path = testDirPath.resolve(Integer.toString(i));
        path.createFile();
        Assert.assertTrue(path.exists());
        return path;
    }
}
