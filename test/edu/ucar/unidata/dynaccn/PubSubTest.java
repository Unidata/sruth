package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
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

public class PubSubTest {
    /**
     * The client task service used by this class.
     */
    private static ExecutorCompletionService<Void> executorService     = new ExecutorCompletionService<Void>(
                                                                               Executors
                                                                                       .newCachedThreadPool());
    /**
     * The maximum size of a created file in bytes.
     */
    private static int                             MAX_SIZE            = 110000;                               // largest
    // NEXRAD2
    // file
    /**
     * The sleep interval to be used.
     */
    private static long                            sleepAmount         = 500;
    /**
     * The root of the publisher file-tree.
     */
    private static final Path                      PUB_ROOT            = Paths
                                                                               .get("/tmp/dynaccn/publisher");
    /**
     * The root of the subscriber file-trees.
     */
    private static final Path                      SUB_ROOT            = Paths
                                                                               .get("/tmp/dynaccn/subscriber");
    /**
     * The number of subscribers.
     */
    private static final int                       SUB_COUNT           = 1;
    /**
     * The pathnames of the root directories of the subscribers.
     */
    private static final Path[]                    absSubRootDirs      = new Path[SUB_COUNT];
    /**
     * The number of pre-subscription files.
     */
    private static final int                       PRE_SUB_FILE_COUNT  = 20;
    /**
     * The number of post-subscription files.
     */
    private static final int                       POST_SUB_FILE_COUNT = 20;
    /**
     * Pseudo random number generator.
     */
    private static final Random                    random              = new Random();
    /**
     * The relative pathnames of the pre-subscription files.
     */
    private static final Path[]                    prePaths            = new Path[PRE_SUB_FILE_COUNT];
    /**
     * The relative pathnames of the post-subscription files.
     */
    private static final Path[]                    postPaths           = new Path[POST_SUB_FILE_COUNT];

    /**
     * Executes a system command.
     * 
     * @param cmd
     *            The command to execute
     * @throws IOException
     * @throws InterruptedException
     */
    private static void system(final String... cmd) throws IOException,
            InterruptedException {
        final ProcessBuilder builder = new ProcessBuilder(cmd);
        builder.inheritIO();
        final Process process = builder.start();
        Assert.assertNotNull(process);
        final int status = process.waitFor();
        Assert.assertEquals(0, status);
    }

    /**
     * Constructs relative pathnames. The choice of parent directory is random.
     */
    private static void constructPaths(final int offset, final Path[] paths) {
        final StringBuilder pathBuilder = new StringBuilder();
        for (int i = 0; i < paths.length; i++) {
            if (random.nextBoolean()) {
                pathBuilder.append("subdir/");
            }
            paths[i] = Paths.get(pathBuilder.append(offset + i).toString());
            pathBuilder.setLength(0);
        }
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        /*
         * Recreate publisher and subscriber directories.
         */
        system(new String[] { "rm", "-rf", PUB_ROOT.toString(),
                SUB_ROOT.toString() });
        Files.createDirectories(PUB_ROOT);
        for (int i = 0; i < SUB_COUNT; i++) {
            absSubRootDirs[i] = SUB_ROOT.resolve(Integer.toString(i + 1));
            Files.createDirectories(absSubRootDirs[i]);
        }
        /*
         * Construct pathnames of pre- and post-subscription files.
         */
        constructPaths(0, prePaths);
        constructPaths(PRE_SUB_FILE_COUNT, postPaths);
        /*
         * Get sleep amount.
         */
        final String sleepAmountString = System.getProperty(PubSubTest.class
                .getName()
                + ".sleepAmount");
        if (null != sleepAmountString) {
            sleepAmount = Long.valueOf(sleepAmountString);
        }
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    /**
     * Creates a file with a given relative pathname. The size of the file is
     * random.
     * 
     * @param publisher
     *            The publisher of the file.
     * @param relPath
     *            The pathname of the file relative to the root of the archive.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private void createFile(final Publisher publisher, final Path relPath)
            throws IOException {
        final PubFile pubFile = publisher.newPubFile(relPath);
        final int size = random.nextInt(MAX_SIZE);
        final byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        pubFile.write(ByteBuffer.wrap(bytes));
        pubFile.publish();
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
     *            The future of the task to stop or {@code null}.
     */
    private static void stop(final Future<Void> future) {
        if (future != null) {
            future.cancel(true);
        }
    }

    /**
     * Verifies that a file was successfully conveyed to all subscribers.
     * 
     * @param path
     *            The pathname of the file relative to the root of the
     *            file-tree.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private void verifyFile(final Path path) throws IOException,
            InterruptedException {
        final String pubPath = PUB_ROOT.resolve(path).toString();
        for (final Path rootDir : absSubRootDirs) {
            system("diff", pubPath, rootDir.resolve(path).toString());
        }
        System.err.println("Successfully conveyed file \"" + path + "\"");
    }

    @Test
    public void testPubSub() throws IOException, InterruptedException,
            ExecutionException {
        /*
         * Create and start the publisher.
         */
        final Publisher publisher = new Publisher(PUB_ROOT, PortNumberSet.ZERO);
        final Future<Void> pubFuture = start(publisher);
        /*
         * Publish some files before the subscribers are started.
         */
        for (final Path path : prePaths) {
            createFile(publisher, path);
        }
        /*
         * Create the subscribers.
         */
        final List<Subscriber> subscribers = new LinkedList<Subscriber>();
        for (final Path rootDir : absSubRootDirs) {
            subscribers.add(new Subscriber(rootDir, publisher
                    .getTrackerAddress(), Predicate.EVERYTHING));
        }
        /*
         * Start the subscribers.
         */
        final List<Future<Void>> subFutures = new LinkedList<Future<Void>>();
        for (final Subscriber subscriber : subscribers) {
            subFutures.add(start(subscriber));
        }

        Thread.sleep(sleepAmount);
        // Thread.sleep(Long.MAX_VALUE);
        /*
         * Verify transmission of published files.
         */
        for (final Path path : prePaths) {
            verifyFile(path);
        }
        /*
         * Publish some files after the subscribers are started.
         */
        for (final Path path : postPaths) {
            createFile(publisher, path);
        }

        Thread.sleep(sleepAmount);
        // Thread.sleep(Long.MAX_VALUE);

        for (final Path path : postPaths) {
            verifyFile(path);
        }
        /*
         * Stop the publisher and subscribers.
         */
        stop(pubFuture);
        for (final Future<Void> future : subFutures) {
            stop(future);
        }
    }

    @Test
    public void testDynamicNetworking() throws IOException,
            InterruptedException, ExecutionException {
        final int MAX_SUBSCRIBERS = 3;
        final int SLEEP_TIME = 2000;
        final String SUBSCRIBER_ARCHIVE_PREFIX = "/tmp/SubscriberApp";
        final int ROUND_COUNT = 3;
        /*
         * Create and start the publisher, using the LDM output directory as the
         * archive.
         */
        final Path pubRoot = Paths.get("/tmp/publisher");
        final Publisher publisher = new Publisher(pubRoot, PortNumberSet.ZERO);
        final Future<Void> pubFuture = start(publisher);

        final List<Future<Void>> subFutures = new ArrayList<Future<Void>>(
                MAX_SUBSCRIBERS);
        for (int round = 0; round < ROUND_COUNT; round++) {
            /*
             * For each subscriber:
             */
            for (int subIndex = 0; subIndex < MAX_SUBSCRIBERS; subIndex++) {
                int prevClientCount = publisher.getClientCount();
                // Stop the previous instance if it exists.
                if (subIndex < subFutures.size()) {
                    stop(subFutures.get(subIndex));
                    subFutures.remove(subIndex);
                    Thread.sleep(SLEEP_TIME);
                    Assert.assertEquals(prevClientCount - 1, publisher
                            .getClientCount());
                    prevClientCount--;
                }
                final Path subRoot = Paths.get(SUBSCRIBER_ARCHIVE_PREFIX
                        + (subIndex + 1));
                final Subscriber subscriber = new Subscriber(subRoot, publisher
                        .getTrackerAddress(), Predicate.EVERYTHING);
                subFutures.add(subIndex, start(subscriber));
                Thread.sleep(SLEEP_TIME);
                Assert.assertEquals(prevClientCount + 1, publisher
                        .getClientCount());
            }
        }

        /*
         * Stop all subscribers.
         */
        for (final Future<Void> subFuture : subFutures) {
            stop(subFuture);
        }

        /*
         * Stop the publisher.
         */
        stop(pubFuture);
    }
}
