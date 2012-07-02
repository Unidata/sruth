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
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
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
     * The logging facility
     */
    private static final Logger     logger              = Util.getLogger();
    /**
     * The test directory.
     */
    private static final Path       TESTDIR             = Paths.get(
                                                                System.getProperty("java.io.tmpdir"))
                                                                .resolve(
                                                                        PubSubTest.class
                                                                                .getSimpleName());
    /**
     * The maximum size of a created file in bytes. A large NEXRAD2 file is
     * about 110000 octets.
     */
    private static int              MAX_SIZE            = 110000;
    /**
     * The root of the publisher file-tree.
     */
    private static final Path       PUB_ROOT            = TESTDIR
                                                                .resolve("publisher");
    /**
     * The root of the subscriber file-trees.
     */
    private static final Path       SUB_ROOT            = TESTDIR
                                                                .resolve("subscribers");
    /**
     * The data directory for ease of transmission verification via diff(1)
     * relative to the root of the archive directory.
     */
    private static final Path       DATA_DIR            = Paths.get("DATA");
    /**
     * The publisher data directory.
     */
    private static final Path       PUB_DATA_DIR        = PUB_ROOT
                                                                .resolve(DATA_DIR);
    /**
     * The number of subscribers.
     */
    private static final int        SUB_COUNT           = 3;
    /**
     * The number of rounds of subscribers in {@link #testDynamicNetworking}.
     */
    private final int               ROUND_COUNT         = 3;

    /**
     * The number of pre-subscription files.
     */
    private static final int        PRE_SUB_FILE_COUNT  = 3;
    /**
     * The number of post-subscription files.
     */
    private static final int        POST_SUB_FILE_COUNT = 3;
    /**
     * The sleep interval, in milliseconds, to be used before checking that the
     * files have been successfully conveyed.
     */
    private static long             sleepAmount         = 2000;
    /**
     * The pathnames of the root directories of the subscribers.
     */
    private static final Path[]     absSubRootDirs      = new Path[SUB_COUNT];
    /**
     * Pseudo random number generator.
     */
    private static final Random     random              = new Random();
    /**
     * The number of constructed paths.
     */
    private static int              pathCount           = 0;
    /**
     * The canceling execution service for tasks.
     */
    private CancellingExecutor      executorService;
    /**
     * The completion service for tasks
     */
    private CompletionService<Void> completionService;

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
         * Create publisher and subscriber directories.
         */
        Files.createDirectories(PUB_ROOT);
        for (int i = 0; i < absSubRootDirs.length; i++) {
            absSubRootDirs[i] = SUB_ROOT.resolve(Integer.toString(i + 1));
            Files.createDirectories(absSubRootDirs[i]);
        }
        /*
         * Create the task-execution service
         */
        executorService = new CancellingExecutor(0, Integer.MAX_VALUE, 0,
                TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
        completionService = new ExecutorCompletionService<Void>(executorService);
        completionService.submit(Misc.newReportingTask(completionService));
    }

    @After
    public void tearDown() throws Exception {
        executorService.shutdownNow();
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
     * @throws FileInfoMismatchException
     *             if the file-information is inconsistent with that of an
     *             existing archive-file
     */
    private static void publishFile(final Publisher publisher)
            throws IOException, FileInfoMismatchException {
        final int size = random.nextInt(MAX_SIZE);
        final byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        final ByteBuffer buf = ByteBuffer.wrap(bytes);
        publisher.publish(nextPath(), buf, -1);
    }

    /**
     * Starts a task.
     */
    private Future<Void> start(final Callable<Void> task) {
        return completionService.submit(task);
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
        future.cancel(true);
        if (!future.isCancelled()) {
            try {
                future.get();
            }
            catch (final ExecutionException e) {
                final Throwable cause = e.getCause();
                logger.error("Unexpected error", cause);
                throw new RuntimeException("Unexpected error", cause);
            }
        }
    }

    private static void diffDirs(final Path subRoot) throws IOException,
            InterruptedException {
        if (PUB_DATA_DIR.toFile().exists()) {
            final int status = Misc.system(true, "diff", "-r", PUB_DATA_DIR
                    .toString(), subRoot.resolve(DATA_DIR).toString());
            assertEquals(0, status);
        }
    }

    private static void deleteDir(final Path dir) throws IOException,
            InterruptedException {
        final int status = Misc.system("rm", "-rf", dir.toString());
        assertEquals(0, status);
    }

    @Test
    public void testNoTracker() throws InterruptedException, IOException {
        final InetSocketAddress trackerAddress = new InetSocketAddress(0);
        final Subscriber subscriber = new Subscriber(absSubRootDirs[0],
                trackerAddress, Predicate.EVERYTHING, new Processor());
        final Future<Void> subscriberFuture = start(subscriber);
        Thread.sleep(sleepAmount);
        stop(subscriberFuture);
    }

    @Test
    public void testRapidNewDirectoryPublishing() throws IOException,
            InterruptedException, FileInfoMismatchException {
        /*
         * Create and start the publisher.
         */
        final Publisher publisher = new Publisher(PUB_ROOT, 0, 0, 0);
        final Future<Void> pubFuture = start(publisher);
        publisher.waitUntilRunning();

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
            subscriber.waitUntilRunning();
        }

        Thread.sleep(sleepAmount);
        // Thread.sleep(Long.MAX_VALUE);

        /*
         * Quickly publish some files in a new subdirectory.
         */
        final Path dir = DATA_DIR.resolve("subdir");
        final byte[] bytes = new byte[1];
        for (int i = 0; i < 3; i++) {
            final Path path = dir.resolve(Integer.toString(i));
            random.nextBytes(bytes);
            final ByteBuffer buf = ByteBuffer.wrap(bytes);
            publisher.publish(new ArchivePath(path), buf, -1);
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

    @Test
    public void testPubSub() throws IOException, InterruptedException,
            ExecutionException, FileInfoMismatchException {
        /*
         * Create the publisher.
         */
        final Publisher publisher = new Publisher(PUB_ROOT, 0, 0, 0);

        System.out.println("Tracker address: " + publisher.getTrackerAddress());
        System.out.println("Source address: " + publisher.getSourceAddress());

        /*
         * Publish some files before the publisher is started.
         */
        for (int i = 0; i < PRE_SUB_FILE_COUNT; i++) {
            publishFile(publisher);
        }

        /*
         * Start the publisher.
         */
        final Future<Void> pubFuture = start(publisher);
        publisher.waitUntilRunning();

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
            subscriber.waitUntilRunning();
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

    @Test
    public void testPublisherRestart() throws IOException,
            InterruptedException, ExecutionException, FileInfoMismatchException {
        /*
         * Create and start the publisher.
         */
        Publisher publisher = new Publisher(PUB_ROOT, 0, 0, 0);
        Future<Void> pubFuture = start(publisher);
        publisher.waitUntilRunning();

        final InetSocketAddress trackerAddress = publisher.getTrackerAddress();

        System.out.println("Tracker address: " + trackerAddress);
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
            subscribers.add(new Subscriber(rootDir, trackerAddress,
                    Predicate.EVERYTHING, new Processor()));
        }
        /*
         * Start the subscribers.
         */
        final List<Future<Void>> subFutures = new LinkedList<Future<Void>>();
        for (final Subscriber subscriber : subscribers) {
            subFutures.add(start(subscriber));
            subscriber.waitUntilRunning();
        }

        Thread.sleep(sleepAmount);
        // Thread.sleep(Long.MAX_VALUE);

        /*
         * Restart the publisher.
         */
        stop(pubFuture);
        publisher.awaitCompletion();
        Thread.sleep(sleepAmount);
        publisher = new Publisher(PUB_ROOT, trackerAddress.getPort(), 0, 0);
        pubFuture = start(publisher);
        publisher.waitUntilRunning();

        // Give the subscribers sufficient time to notice the new publisher
        Thread.sleep(4 * sleepAmount);

        /*
         * Publish some more files.
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
            InterruptedException, ExecutionException, FileInfoMismatchException {
        /*
         * Create and start the publisher.
         */
        final Publisher publisher = new Publisher(PUB_ROOT, 0, 0, 0);
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
                    deleteDir(subRoot);
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
                final Subscriber subscriber = new Subscriber(subRoot,
                        publisher.getTrackerAddress(), Predicate.EVERYTHING,
                        new Processor());
                future = start(subscriber);
                assertNotNull(future);
                subscriber.waitUntilRunning();
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
