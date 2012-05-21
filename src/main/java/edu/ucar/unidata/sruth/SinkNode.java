/**
 * Copyright 2011 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;

import edu.ucar.unidata.sruth.Archive.DistributedTrackerFiles;

/**
 * An interior or leaf node of a distribution graph. A sink-node has a
 * {@link Server} and one or more {@link ClientManager}-s.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class SinkNode extends AbstractNode {
    /**
     * The address of the tracker.
     */
    private final InetSocketAddress        trackerAddress;
    /**
     * The set of client managers (one per desired-data filter).
     */
    @GuardedBy("this")
    private final ArrayList<ClientManager> clientManagers;
    /**
     * The proxy for the tracker
     */
    private final TrackerProxy             trackerProxy;
    /**
     * The logging service.
     */
    private static final Logger            logger = Util.getLogger();
    /**
     * The task execution service
     */
    private final ExecutorService          executorService;

    /**
     * Constructs from a data archive, a specification of the locally-desired
     * data, and the address of the tracker. This constructor is equivalent to
     * {@link #SinkNode(Archive, Predicate, InetSocketAddress, InetSocketAddressSet)
     * SinkNode(archive, predicate, trackerAddress, new InetSocketAddressSet())}
     * 
     * @param archive
     *            The data archive.
     * @param predicate
     *            Specification of the locally-desired data.
     * @param trackerAddress
     *            The address of the tracker.
     * @throws IOException
     *             if an unused port in the given range couldn't be found.
     * @throws NullPointerException
     *             if {@code archive == null}.
     * @throws NullPointerException
     *             if {@code predicate == null}.
     * @throws NullPointerException
     *             if {@code trackerAddress == null}.
     * @see {@link #SinkNode(Archive, Predicate, TrackerAddress, PortNumberSet)}
     */
    SinkNode(final Archive archive, final Predicate predicate,
            final InetSocketAddress trackerAddress) throws IOException {
        this(archive, predicate, trackerAddress, new InetSocketAddressSet());
    }

    /**
     * Constructs from a data archive, a specification of the locally-desired
     * data, and a set of candidate Internet socket addresses for the local
     * server.
     * <p>
     * 
     * @param archive
     *            The data archive.
     * @param predicate
     *            Specification of the locally-desired data.
     * @param trackerAddress
     *            The address of the tracker.
     * @param inetSockAddrSet
     *            The set of candidate Internet socket addresses for the server.
     * @throws IOException
     *             if an unused port in the given range couldn't be found.
     * @throws NullPointerException
     *             if
     *             {@code archive == null || predicate == null || inetSockAddrSet ==
     *             null || trackerAddress == null}.
     * @throws SocketException
     *             if a server socket couldn't be created.
     */
    SinkNode(final Archive archive, final Predicate predicate,
            final InetSocketAddress trackerAddress,
            final InetSocketAddressSet inetSockAddrSet) throws IOException {
        super(archive, predicate, inetSockAddrSet);
        if (trackerAddress == null) {
            throw new NullPointerException();
        }
        this.trackerAddress = trackerAddress;

        final DistributedTrackerFiles distributedTrackerFiles = clearingHouse
                .getDistributedTrackerFiles(trackerAddress);
        trackerProxy = new TrackerProxy(trackerAddress,
                localServer.getSocketAddress(), distributedTrackerFiles);
        clientManagers = new ArrayList<ClientManager>(getPredicate()
                .getFilterCount());
        synchronized (this) {
            for (final Filter filter : getPredicate()) {
                final ClientManager clientManager = new ClientManager(
                        localServer.getSocketAddress(), clearingHouse, filter,
                        trackerProxy);
                clientManagers.add(clientManager);
            }
        }
        executorService = new CancellingExecutor(1, 1 + clientManagers.size(),
                0, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
    }

    @Override
    Server createServer(final ClearingHouse clearingHouse,
            final InetSocketAddressSet inetSockAddrSet) throws SocketException,
            IOException {
        return new SinkServer(clearingHouse, inetSockAddrSet);
    }

    /**
     * Executes this instance. Returns normally if and only if all data was
     * received.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if a severe I/O error occurs
     */
    public Void call() throws InterruptedException, IOException {
        logger.trace("Starting up: {}", this);
        final String prevName = Thread.currentThread().getName();
        Thread.currentThread().setName(toString());
        try {
            int clientManagerCount;
            synchronized (this) {
                clientManagerCount = clientManagers.size();
            }
            try {
                final ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<Void>(
                        executorService);
                final Map<Future<Void>, ClientManager> clientManagerMap = new HashMap<Future<Void>, ClientManager>(
                        clientManagerCount);
                final Future<Void> serverFuture = completionService
                        .submit(localServer);
                synchronized (this) {
                    for (final ClientManager clientManager : clientManagers) {
                        final Future<Void> future = completionService
                                .submit(clientManager);
                        clientManagerMap.put(future, clientManager);
                    }
                }
                while (clientManagerMap.size() > 0) {
                    final Future<Void> future = completionService.take();
                    if (future == serverFuture) {
                        /*
                         * The local {@link SinkServer} completed. Ideally,
                         * because it was cancelled.
                         */
                        if (!future.isCancelled()) {
                            // The local {@link SinkServer} crashed
                            try {
                                future.get();
                                throw new AssertionError();
                            }
                            catch (final ExecutionException e) {
                                final Throwable cause = e.getCause();
                                if (cause instanceof IOException) {
                                    throw new IOException(
                                            "Local sink-server crashed: "
                                                    + localServer, cause);
                                }
                                throw new RuntimeException("Unexpected error: "
                                        + localServer, cause);
                            }
                        }
                    }
                    else {
                        /*
                         * A {@link ClientManager} completed. Ideally, because
                         * all filter-specific data was received.
                         */
                        final ClientManager clientManager = clientManagerMap
                                .remove(future);
                        assert future != null;
                        if (future.isCancelled()) {
                            logger.debug("Cancelled: {}", clientManager);
                        }
                        else {
                            try {
                                future.get();
                                // All filter-specific data was received
                            }
                            catch (final ExecutionException e) {
                                final Throwable cause = e.getCause();
                                if (cause instanceof IOException) {
                                    /*
                                     * We're not talking about a simple
                                     * networking error here
                                     */
                                    throw new IOException("I/O error: "
                                            + clientManager, cause);
                                }
                                throw new RuntimeException("Unexpected error: "
                                        + clientManager, cause);
                            }
                        }
                    }
                }
            }
            finally {
                executorService.shutdownNow();
                awaitCompletion();
            }
        }
        finally {
            Thread.currentThread().setName(prevName);
            logger.trace("Done: {}", this);
        }
        return null;
    }

    /**
     * Waits until this instance is running.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted
     */
    void waitUntilRunning() throws InterruptedException {
        localServer.waitUntilRunning();
        synchronized (this) {
            for (final ClientManager clientManager : clientManagers) {
                clientManager.waitUntilRunning();
            }
        }
    }

    /**
     * Waits until this instance has completed.
     */
    void awaitCompletion() throws InterruptedException {
        Thread.interrupted();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }

    /**
     * Returns the number of received files since {@link #call()} was called.
     * 
     * @return The number of received files since {@link #call()} was called.
     */
    long getReceivedFileCount() {
        return clearingHouse.getReceivedFileCount();
    }

    @Override
    int getClientCount() {
        int n = 0;
        synchronized (this) {
            for (final ClientManager clientManager : clientManagers) {
                n += clientManager.getClientCount();
            }
        }
        return n;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public synchronized String toString() {
        return "SinkNode [localServer=" + localServer + ", trackerAddress="
                + trackerAddress + "]";
    }
}
