/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test of the {@link DelayedPathActionQueue} class.
 * 
 * @author Steven R. Emmerson
 */
public class FileDeleterTest {
    /**
     * Pathname of the file-deletion test-directory.
     */
    private static Path            testDirPath  = Paths.get("/tmp/DelayedPathActionQueue");
    /**
     * Pathname of the file-deletion queue.
     */
    private static Path            queuePath    = testDirPath.resolve("queue");
    /**
     * The file-deleter.
     */
    private DelayedPathActionQueue delayedPathActionQueue;
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
        Files.createDirectory(testDirPath);
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
        Files.deleteIfExists(queuePath);
        delayedPathActionQueue = new DelayedPathActionQueue(testDirPath,
                new PathDelayQueue(queuePath),
                new DelayedPathActionQueue.Action() {
                    @Override
                    void act(final Path path) throws IOException {
                        Files.delete(path);
                    }

                    @Override
                    public String toString() {
                        return "DELETE";
                    }
                });
        Files.createFile(testFilePath);
        Assert.assertTrue(Files.exists(testFilePath));
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        Files.deleteIfExists(testFilePath);
        delayedPathActionQueue.stop();
        Files.deleteIfExists(queuePath);
    }

    /**
     * @throws IOException
     */
    @Test
    public final void testFileDeleter() throws IOException {
        Assert.assertNotNull(delayedPathActionQueue);
        Assert.assertTrue(Files.exists(queuePath));
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.sruth.DelayedPathActionQueue#actUponEventurally(java.nio.file.Path, long)}
     * .
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public final void testDeleteNoWait() throws IOException,
            InterruptedException {
        delayedPathActionQueue.actUponEventurally(testFilePath, 0);
        delayedPathActionQueue.waitUntilEmpty();
        Assert.assertFalse(Files.exists(testFilePath));
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.sruth.DelayedPathActionQueue#actUponEventurally(java.nio.file.Path, long)}
     * .
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public final void testDeleteWait() throws IOException, InterruptedException {
        delayedPathActionQueue.actUponEventurally(testFilePath, 1);
        delayedPathActionQueue.waitUntilEmpty();
        Assert.assertFalse(Files.exists(testFilePath));
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.sruth.DelayedPathActionQueue#getPendingCount()}.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public final void testGetPendingCount() throws IOException,
            InterruptedException {
        Assert.assertEquals(0, delayedPathActionQueue.getPendingCount());
        delayedPathActionQueue.actUponEventurally(testFilePath, 1);
        Assert.assertEquals(1, delayedPathActionQueue.getPendingCount());
        delayedPathActionQueue.waitUntilEmpty();
        Assert.assertEquals(0, delayedPathActionQueue.getPendingCount());
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.sruth.DelayedPathActionQueue#getActedUponCount()}
     * .
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public final void testGetDeletedCount() throws IOException,
            InterruptedException {
        Assert.assertEquals(0, delayedPathActionQueue.getActedUponCount());
        delayedPathActionQueue.actUponEventurally(testFilePath, 1);
        delayedPathActionQueue.waitUntilEmpty();
        Assert.assertEquals(1, delayedPathActionQueue.getActedUponCount());
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
            delayedPathActionQueue.actUponEventurally(createFile(i), 1);
        }
        delayedPathActionQueue.waitUntilEmpty();
        Assert.assertEquals(0, delayedPathActionQueue.getPendingCount());
        Assert.assertEquals(PERFORMANCE_FILE_COUNT,
                delayedPathActionQueue.getActedUponCount());
        final Long stopTime = System.currentTimeMillis();
        System.out.println("Number of files: " + PERFORMANCE_FILE_COUNT);
        final double interval = (stopTime - startTime) / 1000.0;
        System.out.println("Amount of time:  " + interval + " s");
        System.out.println("Deletion rate: "
                + (PERFORMANCE_FILE_COUNT / interval) + "/s");
    }

    private Path createFile(final int i) throws IOException {
        final Path path = testDirPath.resolve(Integer.toString(i));
        Files.createFile(path);
        Assert.assertTrue(Files.exists(path));
        return path;
    }
}
