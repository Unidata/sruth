/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
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
    private static final Logger                   logger          = LoggerFactory
                                                                          .getLogger(Subscriber.class);
    /**
     * The source-node.
     */
    private final SourceNode                      sourceNode;
    /**
     * The tracker.
     */
    private final Tracker                         tracker;
    /**
     * The {@link ExecutorService} for the tracker and source-node tasks.
     */
    private final ExecutorService                 executorService = new CancellingExecutor(
                                                                          2,
                                                                          2,
                                                                          0,
                                                                          TimeUnit.SECONDS,
                                                                          new SynchronousQueue<Runnable>());
    /**
     * The task manager.
     */
    private final ExecutorCompletionService<Void> taskManager     = new ExecutorCompletionService<Void>(
                                                                          executorService);
    /**
     * The data archive.
     */
    private final Archive                         archive;

    /**
     * Constructs from the pathname of the root of the file-tree, the tracker
     * port number, and a range of candidate port numbers for the tracker and
     * server.
     * 
     * @param rootDir
     *            Pathname of the root of the file-tree.
     * @param portSet
     *            The set of candidate port numbers.
     * @throws IOException
     *             if an unused port in the given range couldn't be found.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code rootDir == null || predicate == null || portSet ==
     *             null}.
     * @throws SocketException
     *             if a server-side socket couldn't be created.
     */
    Publisher(final Path rootDir, final PortNumberSet portSet)
            throws IOException {
        archive = new Archive(rootDir);
        sourceNode = new SourceNode(archive, Predicate.NOTHING, portSet);
        tracker = new Tracker(portSet, sourceNode.getServerInfo());
        sourceNode.addDisconnectListener(new Server.DisconnectListener() {
            @Override
            void clientDisconnected(final ServerInfo serverInfo) {
                tracker.removeServer(serverInfo);
            }
        });
    }

    /**
     * Executes this instance. Never returns normally.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if an IO error occurs.
     */
    public Void call() throws InterruptedException, IOException {
        final String origThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(toString());
        try {
            taskManager.submit(sourceNode);
            taskManager.submit(tracker);
            taskManager.take();
        }
        catch (final RejectedExecutionException e) {
            throw new AssertionError(e);
        }
        finally {
            executorService.shutdownNow();
            try {
                archive.close();
            }
            finally {
                Thread.currentThread().setName(origThreadName);
            }
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
     * Returns the number of clients that this instance is serving.
     * 
     * @return The number of clients that this instance is serving.
     */
    int getClientCount() {
        return sourceNode.getClientCount();
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
                final PortNumberSet portSet = PortNumberSet.getInstance(port,
                        port + 3);
                publisher = new Publisher(rootDir, portSet);
                try {
                    System.out.println(publisher.getTrackerPort());
                    System.out.flush();
                    publisher.call();
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
