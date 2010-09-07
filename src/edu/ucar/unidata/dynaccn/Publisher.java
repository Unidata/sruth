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
     * @param port
     *            Number of the port on which the tracker should listen. If
     *            non-positive, then the port will be chosen by the operating
     *            system.
     * @throws IOException
     *             if an I/O error occurs.
     */
    Publisher(final Path rootDir, final int port) throws IOException {
        final Archive archive = new Archive(rootDir);
        sourceNode = new SourceNode(archive, Predicate.NOTHING);
        tracker = new Tracker(port, sourceNode.getServerInfo());
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
     * Executes an instance of this class. Doesn't return. Writes the tracker
     * port number to the standard output stream.
     * <p>
     * Usage:
     * 
     * <pre>
     * java ... edu.ucar.unidata.dynaccn.Publisher [-port &lt;port&gt;] rootDir
     *     
     * where:
     *   &lt;port&gt;   Number of the port on which the tracker should listen.
     *            If not specified or zero, then the port is chosen by the
     *            operating system.
     *   rootDir  Pathname of the root of the data archive.
     * </pre>
     * <p>
     * Exit status:
     * 
     * <pre>
     * 1  Invalid invocation
     * 2  Error occurred in publisher
     * 3  Publisher was interrupted
     * </pre>
     * 
     * @param args
     *            Invocation arguments.
     */
    public static void main(final String[] args) {
        Path rootDir = null;
        int port = -1;
        int status = 0;

        for (int i = 0; 0 == status && i < args.length; i++) {
            final String arg = args[i];
            if ("-port".equals(arg)) {
                try {
                    port = Integer.parseInt(args[++i]);
                    if (port < 0) {
                        logger.error("Port number is too low: " + port);
                        status = 1;
                    }
                    else if (port >= (1 << 16)) {
                        logger.error("Port number is too high: " + port);
                        status = 1;
                    }
                }
                catch (final IndexOutOfBoundsException e) {
                    logger.error("Port argument is missing");
                    status = 1;
                }
            }
            else {
                if (i + 1 < args.length) {
                    logger.error("Too many arguments");
                    status = 1;
                }
                else {
                    try {
                        rootDir = Paths.get(arg);
                    }
                    catch (final InvalidPathException e) {
                        logger.error(
                                "Invalid pathname of root-directory: \"{}\"",
                                arg);
                        status = 1;
                    }
                }
            }
        }
        if (0 != status) {
            logger.error("Usage: java ... "
                    + Publisher.class.getCanonicalName()
                    + " [-port <port>] rootDir");
        }
        else {
            Publisher publisher = null;
            try {
                publisher = new Publisher(rootDir, port);
                try {
                    System.out.println(publisher.getTrackerPort());
                    System.out.flush();
                    publisher.call();
                }
                catch (final ExecutionException e) {
                    logger.error("Error executing " + publisher, e);
                    status = 2;
                }
                catch (final InterruptedException e) {
                    logger.info("Interrupted: " + publisher);
                    status = 3;
                }
            }
            catch (final Exception e) {
                logger.error("Couldn't create publisher for " + rootDir, e);
                status = 2;
            }
        }
        System.exit(status);
    }
}
