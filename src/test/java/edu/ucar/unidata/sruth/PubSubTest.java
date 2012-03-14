package edu.ucar.unidata.sruth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
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

public class PubSubTest {
    /**
     * The logging service.
     */
    private static final Logger             logger              = Util.getLogger();
    /**
     * The test directory.
     */
    private static final Path               TESTDIR             = Paths.get(
                                                                        System.getProperty("java.io.tmpdir"))
                                                                        .resolve(
                                                                                PubSubTest.class
                                                                                        .getSimpleName());
    /**
     * The maximum size of a created file in bytes. A large NEXRAD2 file is
     * about 110000 octets.
     */
    private static int                      MAX_SIZE            = 110000;
    /**
     * The root of the publisher file-tree.
     */
    private static final Path               PUB_ROOT            = TESTDIR
                                                                        .resolve("publisher");
    /**
     * The root of the subscriber file-trees.
     */
    private static final Path               SUB_ROOT            = TESTDIR
                                                                        .resolve("subscribers");
    /**
     * The data directory for ease of transmission verification via diff(1)
     * relative to the root of the archive directory.
     */
    private static final Path               DATA_DIR            = Paths.get("DATA");
    /**
     * The publisher data directory.
     */
    private static final Path               PUB_DATA_DIR        = PUB_ROOT
                                                                        .resolve(DATA_DIR);
    /**
     * The number of subscribers.
     */
    private static final int                SUB_COUNT           = 3;
    /**
     * The number of rounds of subscribers in {@link #testDynamicNetworking}.
     */
    private final int                       ROUND_COUNT         = 3;

    /**
     * The number of pre-subscription files.
     */
    private static final int                PRE_SUB_FILE_COUNT  = 10;
    /**
     * The number of post-subscription files.
     */
    private static final int                POST_SUB_FILE_COUNT = 10;
    /**
     * The sleep interval, in milliseconds, to be used before checking that the
     * files have been successfully conveyed.
     */
    private static long                     sleepAmount         = 2000;
    /**
     * The canceling execution service for tasks.
     */
    private static final CancellingExecutor executorService     = new CancellingExecutor(
                                                                        0,
                                                                        Integer.MAX_VALUE,
                                                                        0,
                                                                        TimeUnit.SECONDS,
                                                                        new SynchronousQueue<Runnable>());
    /**
     * The pathnames of the root directories of the subscribers.
     */
    private static final Path[]             absSubRootDirs      = new Path[SUB_COUNT];
    /**
     * Pseudo random number generator.
     */
    private static final Random             random              = new Random();
    /**
     * The number of constructed paths.
     */
    private static int                      pathCount           = 0;

    /**
     * Constructs an archive pathname. The choice of a subdirectory or not is
     * random.
     */
    private static ArchivePath nextPath() {
        Path path = DATA_DIR;
        if (random.nextBoolean()) {
            path = path.resolve("subdir");
        }
        path = path.resolve(Integer.toString(pathCount++));
        return new ArchivePath(path);
    }

    private static void removeTestDirectory() throws IOException,
            InterruptedException {
        Misc.system("rm", "-rf", TESTDIR.toString());
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        /*
         * Get sleep amount.
         */
        final String sleepAmountString = System.getProperty(PubSubTest.class
                .getName() + ".sleepAmount");
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
        removeTestDirectory();
        /*
         * Recreate publisher and subscriber directories.
         */
        Files.createDirectories(PUB_ROOT);
        for (int i = 0; i < absSubRootDirs.length; i++) {
            absSubRootDirs[i] = SUB_ROOT.resolve(Integer.toString(i + 1));
            Files.createDirectories(absSubRootDirs[i]);
        }
    }

    @After
    public void tearDown() throws Exception {
    }

    /**
     * Creates a new file. The size of the file is random.
     * 
     * @param publisher
     *            The publisher of the file.
     * @throws FileAlreadyExistsException
     *             the file is being actively written by another thread.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private static void publishFile(final Publisher publisher)
            throws IOException {
        final int size = random.nextInt(MAX_SIZE);
        final byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        final ByteBuffer buf = ByteBuffer.wrap(bytes);
        publisher.publish(nextPath(), buf);
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
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    private static void stop(final Future<Void> future)
            throws InterruptedException {
        if (future != null) {
            future.cancel(true);
            try {
                future.get();
            }
            catch (final CancellationException ignored) {
            }
            catch (final ExecutionException e) {
                final Throwable t = e.getCause();
                logger.error("Execution exception", t);
            }
        }
    }

    private static void diffDirs(final Path subRoot) throws IOException,
            InterruptedException {
        final int status = Misc.system("diff", "-r", PUB_DATA_DIR.toString(),
                subRoot.resolve(DATA_DIR).toString());
        assertEquals(0, status);
    }

    private static void deleteDir(final Path dir) throws IOException,
            InterruptedException {
        final int status = Misc.system("rm", "-rf", dir.toString());
        assertEquals(0, status);
    }

    @Test(expected = java.nio.file.NoSuchFileException.class)
    public void testNoTracker() throws InterruptedException, IOException {
        final InetSocketAddress trackerAddress = new InetSocketAddress(0);
        final Subscriber subscriber = new Subscriber(absSubRootDirs[0],
                trackerAddress, Predicate.EVERYTHING, new Processor());
        subscriber.call();
    }

    @Test
    public void testPubSub() throws IOException, InterruptedException,
            ExecutionException {
        /*
         * Create and start the publisher.
         */
        final Publisher publisher = new Publisher(PUB_ROOT);
        final Future<Void> pubFuture = start(publisher);
        publisher.waitUntilRunning();

        System.out.println("Tracker address: " + publisher.getTrackerAddress());
        System.out.println("Source address: " + publisher.getSourceAddress());

        /*
         * Publish some files before the subscribers are started.
         */
        for (int i = 0; i < PRE_SUB_FILE_COUNT; i++) {
            publishFile(publisher);
        }

        /*
         * Create the subscribers.
         */
        final List<Subscriber> subscribers = new LinkedList<Subscriber>();
        for (final Path rootDir : absSubRootDirs) {
            subscribers
                    .add(new Subscriber(rootDir, publisher.getTrackerAddress(),
                            Predicate.EVERYTHING, new Processor()));
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
         * Publish some files after the subscribers are started.
         */
        for (int i = 0; i < POST_SUB_FILE_COUNT; i++) {
            publishFile(publisher);
        }

        Thread.sleep(sleepAmount);
        // Thread.sleep(Long.MAX_VALUE);

        /*
         * Verify transmission of published files.
         */
        for (final Subscriber subscriber : subscribers) {
            diffDirs(subscriber.getRootDir());
        }

        /*
         * Stop the subscribers and then the publisher.
         */
        for (final Future<Void> future : subFutures) {
            stop(future);
        }
        Thread.sleep(sleepAmount);
        stop(pubFuture);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDynamicNetworking() throws IOException,
            InterruptedException, ExecutionException {
        /*
         * Create and start the publisher.
         */
        final Publisher publisher = new Publisher(PUB_ROOT);
        final Future<Void> pubFuture = start(publisher);
        assertNotNull(pubFuture);
        publisher.waitUntilRunning();

        /*
         * Publish some files before the subscribers are started.
         */
        for (int i = 0; i < PRE_SUB_FILE_COUNT; i++) {
            publishFile(publisher);
        }

        final Future<?>[] subFutures = new Future<?>[SUB_COUNT];
        for (int round = 0; round < ROUND_COUNT; round++) {
            /*
             * For each subscriber:
             */
            for (int subIndex = 0; subIndex < SUB_COUNT; subIndex++) {
                final Path subRoot = absSubRootDirs[subIndex];
                int prevClientCount = publisher.getClientCount();
                Future<Void> future;
                if (round == 0) {
                    assertEquals(prevClientCount, subIndex);
                }
                else {
                    // Stop the subscriber in this slot from the previous round.
                    assertEquals(prevClientCount, SUB_COUNT);
                    future = (Future<Void>) subFutures[subIndex];
                    assertNotNull(future);
                    stop(future);
                    Thread.sleep(sleepAmount);
                    assertEquals(--prevClientCount, publisher.getClientCount());
                }
                deleteDir(subRoot);
                final Subscriber subscriber = new Subscriber(subRoot,
                        publisher.getTrackerAddress(), Predicate.EVERYTHING,
                        new Processor());
                future = start(subscriber);
                assertNotNull(future);
                subFutures[subIndex] = future;
                Thread.sleep(sleepAmount);
                assertEquals(prevClientCount + 1, publisher.getClientCount());
            }

            /*
             * Publish some files after the subscribers are started.
             */
            for (int i = 0; i < POST_SUB_FILE_COUNT; i++) {
                publishFile(publisher);
            }

            Thread.sleep(sleepAmount);
            // Thread.sleep(Long.MAX_VALUE);

            /*
             * Verify transmission
             */
            for (int subIndex = 0; subIndex < SUB_COUNT; subIndex++) {
                final Path subRoot = absSubRootDirs[subIndex];
                diffDirs(subRoot);
            }
        }

        /*
         * Stop all subscribers.
         */
        for (final Future<?> subFuture : subFutures) {
            stop((Future<Void>) subFuture);
        }

        /*
         * Stop the publisher.
         */
        stop(pubFuture);
    }
}
