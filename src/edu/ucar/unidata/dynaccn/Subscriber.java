/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import net.jcip.annotations.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A subscriber of data.
 * 
 * Instances are thread-compatible but not thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@NotThreadSafe
final class Subscriber implements Callable<Void> {
    /**
     * The logger for this class.
     */
    private static final Logger logger    = LoggerFactory
                                                  .getLogger(Subscriber.class);
    /**
     * The proxy for the tracker.
     */
    private final TrackerProxy  trackerProxy;
    /**
     * The sink-node.
     */
    private final SinkNode      sinkNode;
    /**
     * The data-selection predicate.
     */
    private final Predicate     predicate;
    /**
     * Whether or not this instance is running.
     */
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    /**
     * The archive.
     */
    private final Archive       archive;

    /**
     * Constructs from the pathname of the file-tree, the socket address of the
     * tracker, and the predicate for the desired data.
     * 
     * @param rootDir
     *            Pathname of the root of the file-tree.
     * @param trackerAddress
     *            The socket address of the tracker.
     * @param prediate
     *            The predicate for selecting the desired data.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code rootDir == null || trackerProxy == null ||
     *             predicate == null}.
     */
    Subscriber(final Path rootDir, final SocketAddress trackerAddress,
            final Predicate predicate) throws IOException {
        this(rootDir, new TrackerProxy(trackerAddress), predicate);
    }

    /**
     * Constructs from the pathname of the file-tree, information about the
     * tracker, and the predicate for the desired data.
     * 
     * @param rootDir
     *            Pathname of the root of the file-tree.
     * @param trackerProxy
     *            Information on the tracker.
     * @param prediate
     *            The predicate for selecting the desired data.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code rootDir == null || trackerProxy == null ||
     *             predicate == null}.
     */
    Subscriber(final Path rootDir, final TrackerProxy trackerProxy,
            final Predicate predicate) throws IOException {
        if (null == rootDir) {
            throw new NullPointerException();
        }
        if (null == trackerProxy) {
            throw new NullPointerException();
        }
        if (null == predicate) {
            throw new NullPointerException();
        }
        archive = new Archive(rootDir);
        sinkNode = new SinkNode(archive, predicate);
        this.trackerProxy = trackerProxy;
        this.predicate = predicate;
    }

    /**
     * Returns a proxy for the tracker.
     * 
     * @return A proxy for the tracker.
     */
    TrackerProxy getTrackerProxy() {
        return trackerProxy;
    }

    /**
     * Returns the predicate used by this instance.
     * 
     * @return The predicate used by this instance.
     */
    Predicate getPredicate() {
        return predicate;
    }

    /**
     * Returns the pathname of the root of the archive.
     * 
     * @return The pathname of the root of the archive.
     */
    Path getRootDir() {
        return archive.getRootDir();
    }

    /**
     * Executes this instance. Returns normally if and only if all desired data
     * has been received.
     * 
     * @throws ClassNotFoundException
     *             if the reply from the tracker can't be understood.
     * @throws ExecutionException
     *             if a subtask terminates due to an error.
     * @throws IllegalStateException
     *             if {@link #isRunning()}.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if an I/O error occurs.
     * @see {@link #isRunning()}
     */
    public Void call() throws ExecutionException, InterruptedException,
            ClassNotFoundException, IOException {
        if (!isRunning.compareAndSet(false, true)) {
            throw new IllegalStateException();
        }
        final String origThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(toString());
        try {
            final Inquisitor inquisitor = new Inquisitor(sinkNode
                    .getServerInfo(), predicate);
            final Plumber plumber = trackerProxy.getPlumber(inquisitor);
            plumber.connect(sinkNode);
            sinkNode.call();
        }
        finally {
            Thread.currentThread().setName(origThreadName);
        }
        return null;
    }

    /**
     * Indicates if this instance is running (i.e., whether or not
     * {@link #call()} has been called).
     * 
     * @return {@code true} if and only if {@link #call()} has been called.
     */
    boolean isRunning() {
        return isRunning.get();
    }

    /**
     * Returns the number of received files since {@link #call()} was called.
     * 
     * @return The number of received files since {@link #call()} was called.
     */
    public long getReceivedFileCount() {
        return sinkNode.getReceivedFileCount();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Subscriber [sinkNode=" + sinkNode + ", trackerProxy="
                + trackerProxy + "]";
    }

    /**
     * Executes an instance of this class.
     * <p>
     * Exit status:
     * 
     * <pre>
     *   0  Success: data received and process terminated.
     *   1  Invalid invocation syntax
     *   2  Invalid pathname of the root directory
     *   3  Invalid pathname of the subscription file
     *   4  Couldn't get subscription from subscription file
     *   5  Error receiving data
     * </pre>
     * 
     * @param args
     *            [0] Pathname of the root of the file-tree.
     * @param args
     *            [1] Pathname of the XML subscription file.
     */
    public static void main(final String[] args) {
        if (2 != args.length) {
            logger.error("Invalid invocation");
            logger.error("Usage: java ... "
                    + Subscriber.class.getCanonicalName()
                    + " rootDir subscriptionPath");
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
        Path subscriptionPath = null;
        try {
            subscriptionPath = Paths.get(args[1]);
        }
        catch (final InvalidPathException e) {
            logger.error("Invalid pathname of subscription file: \"{}\"",
                    args[1]);
            System.exit(3);
        }
        Subscription subscription = null;
        try {
            subscription = new Subscription(subscriptionPath);
        }
        catch (final Exception e) {
            logger.error("Couldn't get subscription from XML file \""
                    + subscriptionPath + "\"", e);
            System.exit(4);
        }
        Subscriber subscriber = null;
        try {
            subscriber = new Subscriber(rootDir, subscription
                    .getTrackerAddress(), subscription.getPredicate());
        }
        catch (final Exception e) {
            logger.error("Couldn't subscribe to " + subscription, e);
            System.exit(5);
        }
        try {
            subscriber.call();
        }
        catch (final Exception e) {
            logger.error("Error executing " + subscriber, e);
            System.exit(5);
        }
        System.exit(0);
    }
}
