/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;

import edu.ucar.unidata.sruth.Archive.DistributedTrackerFiles;

/**
 * A publisher of data. A publisher has a source-node and a tracker.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
public final class Publisher implements Callable<Void> {
    /**
     * The logger for this class.
     */
    private static final Logger                   logger            = Util.getLogger();
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
    private final CancellingExecutor              executor          = new CancellingExecutor(
                                                                            2,
                                                                            2,
                                                                            0,
                                                                            TimeUnit.SECONDS,
                                                                            new SynchronousQueue<Runnable>());
    /**
     * The task manager.
     */
    private final ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<Void>(
                                                                            executor);
    /**
     * The data archive.
     */
    private final Archive                         archive;
    /**
     * Manager of package administration files.
     */
    private final DistributedTrackerFiles         distributedTrackerFiles;

    /**
     * Constructs from the pathname of the root of the file-tree. The tracker
     * and will listen on its IANA-assigned port and the source-node server will
     * listen on an ephemeral port.
     * 
     * @param rootDir
     *            Pathname of the root of the file-tree.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code rootDir == null}.
     * @see #getTrackerAddress()
     * @see #getSourceAddress()
     */
    public Publisher(final Path rootDir) throws IOException {
        this(rootDir, Tracker.IANA_PORT);
    }

    /**
     * Constructs from the pathname of the root of the file-tree and the port on
     * which the tracker will listen. The source-node server will listen on an
     * ephemeral port.
     * 
     * @param rootDir
     *            Pathname of the root of the file-tree
     * @param port
     *            The port on which the tracker will listen or {@code 0}, in
     *            which case an ephemeral port will be chosen by the
     *            operating-system.
     * @throws IOException
     *             if an I/O error occurs
     * @throws IllegalArgumentException
     *             if {@code port < 0}
     * @throws NullPointerException
     *             if {@code rootDir == null}
     * @see #getTrackerAddress()
     * @see #getSourceAddress()
     */
    public Publisher(final Path rootDir, final int port) throws IOException {
        archive = new Archive(rootDir);
        sourceNode = new SourceNode(archive);
        final InetAddress trackerAddress = InetAddress.getLocalHost();
        final InetSocketAddress trackerSocketAddress = new InetSocketAddress(
                trackerAddress, port);
        tracker = new Tracker(sourceNode.getLocalServerSocketAddress(),
                trackerSocketAddress);
        distributedTrackerFiles = archive.getDistributedTrackerFiles(tracker
                .getServerAddress());
        tracker.addNetworkTopologyChangeListener(new PropertyChangeListener() {
            @Override
            public void propertyChange(final PropertyChangeEvent evt) {
                distributedTrackerFiles.distribute((Topology) evt
                        .getNewValue());
            }
        });
        distributedTrackerFiles.distribute(tracker.getReportingAddress());
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
        logger.trace("Starting up: {}", this);
        final String origThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(toString());
        try {
            completionService.submit(sourceNode);
            final Future<Void> trackerFuture = completionService
                    .submit(tracker);
            final Future<Void> future = completionService.take();
            if (!future.isCancelled()) {
                try {
                    future.get();
                    throw new AssertionError();
                }
                catch (final ExecutionException e) {
                    final Throwable cause = e.getCause();
                    final Object task = future == trackerFuture
                            ? tracker
                            : sourceNode;
                    if (cause instanceof IOException) {
                        throw new IOException("IO error: " + task, cause);
                    }
                    throw new RuntimeException("Unexpected error: " + task,
                            cause);
                }
            }
        }
        catch (final RejectedExecutionException e) {
            throw new AssertionError(e);
        }
        finally {
            executor.shutdownNow();
            awaitCompletion();
            try {
                archive.close();
            }
            catch (final IOException ignored) {
            }
            Thread.currentThread().setName(origThreadName);
            logger.trace("Done: {}", this);
        }
        return null;
    }

    /**
     * Waits until this instance is running.
     * <p>
     * This method is potentially slow.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    void waitUntilRunning() throws InterruptedException {
        sourceNode.waitUntilRunning();
        tracker.waitUntilRunning();
    }

    /**
     * Returns the address of the tracker.
     * 
     * @return The address of the tracker.
     */
    public InetSocketAddress getTrackerAddress() {
        return tracker.getServerAddress();
    }

    /**
     * Returns the address of the source-node's server.
     * 
     * @return The address of the source-node's server.
     */
    public InetSocketAddress getSourceAddress() {
        return sourceNode.getLocalServerSocketAddress();
    }

    /**
     * Returns the port on which the tracker listens.
     * 
     * @return The tracker port.
     */
    public int getTrackerPort() {
        return tracker.getServerAddress().getPort();
    }

    /**
     * Publishes data.
     * 
     * @param path
     *            Pathname for the data in the archive.
     * @param data
     *            the data.
     * @param timeToLive
     *            Lifetime of the data in seconds. A negative value means
     *            indefinitely.
     * @throws FileAlreadyExistsException
     *             the file is being actively written by another thread.
     * @throws FileInfoMismatchException
     *             if the file information conflicts with an existing archive
     *             file.
     * @throws IOException
     *             if an I/O error occurs.
     */
    public void publish(final ArchivePath path, final ByteBuffer data,
            final int timeToLive) throws FileAlreadyExistsException,
            IOException, FileInfoMismatchException {
        final byte[] bytes = data.array();
        final InputStream inputStream = new ByteArrayInputStream(bytes);
        final ReadableByteChannel channel = Channels.newChannel(inputStream);
        archive.save(path, channel, timeToLive);
    }

    /**
     * Returns the number of clients that this instance is serving. A "client"
     * in this context is a subscriber that's receiving data directly from this
     * instance's server.
     * 
     * @return The number of clients that this instance is serving.
     */
    public int getClientCount() {
        return sourceNode.getServletCount();
    }

    /**
     * Returns the socket address of the tracker.
     * 
     * @return The socket address of the tracker.
     */
    private SocketAddress getTrackerSocketAddress() {
        return tracker.getSocketAddress();
    }

    /**
     * Returns the absolute pathname of an archive pathname.
     * 
     * @param archivePath
     *            The archive pathname.
     * @return The absolute pathname corresponding to the archive pathname.
     */
    Path getAbsolutePath(final ArchivePath archivePath) {
        return archive.resolve(archivePath);
    }

    /**
     * Waits until this instance has completed.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted
     */
    void awaitCompletion() throws InterruptedException {
        Thread.interrupted();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
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
     * Executes an instance of this class. Doesn't return. Writes the tracker's
     * Internet socket address to the standard output stream.
     * <p>
     * Usage:
     * 
     * <pre>
     * java ... edu.ucar.unidata.sruth.Publisher [-p port] rootDir
     *     
     * where:
     *   -p port  Port number on which the tracker will listen. The default is
     *            an ephemeral port assigned by the operating-system.
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
     * @throws IOException
     *             if an I/O error occurs
     * @throws SecurityException
     *             if a security exception occurs
     */
    public static void main(final String[] args) throws SecurityException,
            IOException {
        int status = 0;
        Path rootDir = null;
        int port = 0;

        /*
         * Decode the command-line.
         */
        try {
            int iarg;
            String arg;

            for (iarg = 0; iarg < args.length; iarg++) {
                arg = args[iarg];
                try {
                    if (arg.charAt(0) != '-') {
                        break;
                    }
                    final String optString = arg.substring(1);
                    arg = args[++iarg];
                    if (optString.equals("p")) {
                        /*
                         * Decode the port argument.
                         */
                        try {
                            port = Integer.valueOf(arg);
                        }
                        catch (final Exception e) {
                            logger.error(
                                    "Couldn't decode port argument: \"{}\": {}",
                                    arg, e.toString());
                            throw new IllegalArgumentException();
                        }
                    }
                    else {
                        logger.error("Invalid option: \"{}\"", optString);
                        throw new IllegalArgumentException();
                    }
                }
                catch (final IndexOutOfBoundsException e) {
                    logger.error("Invalid argument: \"{}\"", arg);
                    throw new IllegalArgumentException();
                }
            }
            /*
             * Process the archive-directory argument.
             */
            if (iarg >= args.length) {
                logger.error("The archive-directory argument is missing");
                throw new IllegalArgumentException();
            }
            else {
                arg = args[iarg];
                try {
                    rootDir = Paths.get(arg);
                }
                catch (final InvalidPathException e) {
                    logger.error("Invalid pathname of root-directory: \"{}\"",
                            arg);
                    throw new IllegalArgumentException();
                }
            }
            if (++iarg < args.length) {
                logger.error("Too many arguments");
                throw new IllegalArgumentException();
            }
        }
        catch (final IllegalArgumentException e) {
            logger.info("Usage: ... [-p port] rootDir");
            status = 1;
        }

        /*
         * Execute the command-line.
         */
        if (status == 0) {
            try {
                final Publisher publisher = new Publisher(rootDir, port);
                try {
                    System.out.println(publisher.getTrackerSocketAddress());
                    System.out.flush();
                    publisher.call();
                }
                catch (final InterruptedException e) {
                    logger.info("Interrupted: " + publisher);
                    status = 3;
                }
            }
            catch (final Exception e) {
                logger.error("Couldn't create publisher for archive \""
                        + rootDir + "\"", e);
                status = 2;
            }
        }

        System.exit(status);
    }
}
