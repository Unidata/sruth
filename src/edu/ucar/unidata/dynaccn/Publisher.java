/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import net.jcip.annotations.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A publisher of data. Runs a source-node and a tracker.
 * 
 * Instances are thread-compatible but not thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@NotThreadSafe
final class Publisher implements Callable<Void> {
    /**
     * The logger for this class.
     */
    private static final Logger     logger          = LoggerFactory
                                                            .getLogger(Subscriber.class);
    /**
     * The source-node.
     */
    private final SourceNode        sourceNode;
    /**
     * The tracker.
     */
    private final Tracker           tracker;
    /**
     * The executorService service.
     */
    private final ExecutorService   executorService = new CancellingExecutor(
                                                            2,
                                                            2,
                                                            0,
                                                            TimeUnit.SECONDS,
                                                            new LinkedBlockingQueue<Runnable>());
    /**
     * The task manager.
     */
    private final TaskManager<Void> taskManager     = new TaskManager<Void>(
                                                            executorService);

    /**
     * Constructs from the pathname of the root of the file-tree.
     * 
     * @param rootDir
     *            Pathname of the root of the file-tree.
     * @throws IOException
     *             if an I/O error occurs.
     */
    Publisher(final Path rootDir) throws IOException {
        final Archive archive = new Archive(rootDir);
        sourceNode = new SourceNode(archive, Predicate.NOTHING);
        tracker = new Tracker(sourceNode.getServerInfo());
    }

    /**
     * Executes this instance. Never returns normally.
     * 
     * @throws ExecutionException
     *             if a task terminates due to an error.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    public Void call() throws InterruptedException, ExecutionException {
        final String origThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(toString());
        try {
            taskManager.submit(sourceNode);
            taskManager.submit(tracker);
            taskManager.waitUpon();
        }
        catch (final RejectedExecutionException e) {
            throw new AssertionError(e);
        }
        finally {
            taskManager.cancel();
            executorService.shutdownNow();
            Thread.currentThread().setName(origThreadName);
        }
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Publisher [sourceNode=" + sourceNode + "]";
    }

    /**
     * Returns the address of the tracker.
     * 
     * @return The address of the tracker.
     */
    InetSocketAddress getTrackerAddress() {
        return tracker.getAddress();
    }

    /**
     * Returns the port on which the tracker listens.
     * 
     * @return The tracker port.
     */
    int getTrackerPort() {
        return tracker.getAddress().getPort();
    }

    /**
     * Returns a handle on a new, unpublished file that's ready for content.
     * 
     * @param path
     *            The pathname of the file relative to the root of the
     *            file-tree.
     * @return A handle on the new file.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws IllegalArgumentException
     *             if {@code path.isAbsolute()}.
     */
    PubFile newPubFile(final Path path) throws IOException {
        return sourceNode.newPubFile(path);
    }

    /**
     * Executes an instance of this class. Doesn't return.
     * <p>
     * Exit status:
     * 
     * <pre>
     *   1  Invalid invocation
     *   2  Invalid pathname of the root directory
     *   3  Error serving data
     * </pre>
     * 
     * @param args
     *            [0] Pathname of the root of the file-tree.
     * @param args
     *            [1] Pathname of the XML subscription file.
     */
    public static void main(final String[] args) {
        if (1 != args.length) {
            logger.error("Invalid invocation");
            logger.error("Usage: java ... "
                    + Publisher.class.getCanonicalName() + " rootDir");
            System.exit(1);
        }
        Path rootDir = null;
        try {
            rootDir = Paths.get(args[0]);
        }
        catch (final InvalidPathException e) {
            logger.error("Invalid pathname of root-directory: \"{}\"", args[0]);
            System.exit(2);
        }
        Publisher publisher = null;
        try {
            publisher = new Publisher(rootDir);
        }
        catch (final Exception e) {
            logger.error("Couldn't create publisher for " + rootDir, e);
            System.exit(3);
        }
        try {
            System.out.println(publisher.getTrackerPort());
            System.out.flush();
            publisher.call();
        }
        catch (final Exception e) {
            logger.error("Error executing " + publisher, e);
            System.exit(3);
        }
        System.exit(0);
    }
}
