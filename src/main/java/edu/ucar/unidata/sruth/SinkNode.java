/**
 * Copyright 2011 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
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
     * The {@link ClientManager} executor service.
     */
    private final CancellingExecutor              executorService;
    /**
     * The executor completion service.
     */
    private final ExecutorCompletionService<Void> completionService;
    /**
     * The thread that executes the server.
     */
    private final Thread                          serverThread;
    /**
     * The address of the tracker.
     */
    private final InetSocketAddress               trackerAddress;
    /**
     * The set of client managers (one per desired-data filter).
     */
    @GuardedBy("itself")
    private final ArrayList<ClientManager>        clientManagers;
    /**
     * The proxy for the tracker
     */
    private final TrackerProxy                    trackerProxy;
    /**
     * The logging service.
     */
    private static final Logger                   logger = Util.getLogger();

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

        executorService = new CancellingExecutor(0, predicate.getFilterCount(),
                0, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
        completionService = new ExecutorCompletionService<Void>(executorService);

        serverThread = new Thread() {
            @Override
            public void run() {
                try {
                    localServer.call();
                }
                catch (final InterruptedException ignored) {
                }
                catch (final IOException e) {
                    logger.error("Server failed: " + localServer, e);
                    executorService.shutdownNow();
                    // SinkNode.this.cancel();
                }
            }
        };

        final DistributedTrackerFiles distributedTrackerFiles = clearingHouse
                .getDistributedTrackerFiles(trackerAddress);
        trackerProxy = new TrackerProxy(trackerAddress,
                localServer.getSocketAddress(), distributedTrackerFiles);
        clientManagers = new ArrayList<ClientManager>(getPredicate()
                .getFilterCount());
        synchronized (clientManagers) {
            for (final Filter filter : getPredicate()) {
                final ClientManager clientManager = new ClientManager(
                        localServer.getSocketAddress(), clearingHouse, filter,
                        trackerProxy);
                clientManagers.add(clientManager);
            }
        }
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
     * @throws NoSuchFileException
     *             if the tracker couldn't be contacted and there's no
     *             tracker-specific topology-file in the archive.
     */
    public Void call() throws InterruptedException, NoSuchFileException {
        final String prevName = Thread.currentThread().getName();
        Thread.currentThread().setName(toString());
        serverThread.start();
        try {
            final int n;
            synchronized (clientManagers) {
                for (final ClientManager clientManager : clientManagers) {
                    completionService.submit(clientManager);
                }
                n = clientManagers.size();
            }
            for (int i = 0; i < n; i++) {
                final Future<Void> future = completionService.take();
                if (!future.isCancelled()) {
                    try {
                        future.get();
                    }
                    catch (final ExecutionException e) {
                        final Throwable t = e.getCause();
                        if (t instanceof InterruptedException) {
                            throw (InterruptedException) t;
                        }
                        if (t instanceof NoSuchFileException) {
                            throw (NoSuchFileException) t;
                        }
                        throw Util.launderThrowable(t);
                    }
                }
            }
        }
        finally {
            localServer.cancel();
            executorService.shutdownNow();
            Thread.currentThread().setName(prevName);
        }
        return null;
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
        synchronized (clientManagers) {
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
