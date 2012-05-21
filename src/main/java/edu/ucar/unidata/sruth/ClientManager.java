/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.prefs.Preferences;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.Immutable;
import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;

import edu.ucar.unidata.sruth.TrackerProxy.FilteredProxy;

/**
 * Manages a set of clients for a specific filter. Populates the set, removes
 * poor performing members, and adds new members as appropriate.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class ClientManager implements Callable<Void> {
    /**
     * The client-creation task.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private final class ClientCreator extends UninterruptibleTask<Void> {
        /**
         * The filter-specific proxy for the tracker
         */
        private final FilteredProxy  filteredProxy;
        /**
         * The "isRunning" latch.
         */
        private final CountDownLatch isRunningLatch = new CountDownLatch(1);

        /**
         * Constructs from nothing.
         */
        ClientCreator() {
            filteredProxy = trackerProxy.getFilteredProxy(ClientManager.this);
        }

        /**
         * Manages clients. Populates the set of clients and replaces poorly
         * performing ones. Registers the client-manager with the
         * filter-specific tracker proxy.
         * <p>
         * This method is potentially slow and uninterruptible
         * 
         * @throws InterruptedException
         *             if the current thread is interrupted
         * @throws SocketException
         *             if {@link #stop()} is called by another thread
         * @throws IOException
         *             if an I/O error occurs
         */
        @Override
        public Void call() throws InterruptedException, IOException {
            int timeout = 0;
            for (boolean registered = false; !registered
                    && !Thread.currentThread().isInterrupted();) {
                try {
                    filteredProxy.register();
                    registered = true;
                }
                catch (final ConnectException e) {
                    logger.debug(
                            "Couldn't connect to tracker {}: {}. Continuing...",
                            trackerProxy.getAddress(), e.toString());
                }
                catch (final InvalidMessageException e) {
                    logger.debug(
                            "Invalid communication with tracker {}: {}. Continuing...",
                            trackerProxy.getAddress(), e.toString());
                }
                timeout = waitUntilDoneOrTimeout(false, timeout);
            }
            try {
                isRunningLatch.countDown();
                while (!Thread.currentThread().isInterrupted()) {
                    if (enoughClients()) {
                        rankClients();
                        do {
                            removeWorstClient();
                        } while (!Thread.currentThread().isInterrupted()
                                && enoughClients());
                    }
                    while (!Thread.currentThread().isInterrupted()
                            && !enoughClients()) {
                        try {
                            if (!addClient()) {
                                timeout = waitUntilDoneOrTimeout(false, timeout);
                            }
                        }
                        catch (final NoSuchFileException e) {
                            timeout = waitUntilDoneOrTimeout(false, timeout);
                            logger.debug("Continuing...");
                        }
                        catch (final IOException e) {
                            logger.warn("Couldn't add new client: {}",
                                    e.toString());
                            timeout = waitUntilDoneOrTimeout(false, timeout);
                        }
                    }
                    if (!Thread.currentThread().isInterrupted()) {
                        restartClientCounters();
                        timeout = waitUntilDoneOrTimeout(true,
                                REPLACEMENT_PERIOD);
                    }
                }
            }
            finally {
                filteredProxy.deregister();
            }
            return null;
        }

        /**
         * Waits until this instance is running.
         * <p>
         * This method is potentially slow.
         * 
         * @throws InterruptedException
         *             if the current thread is interrupted
         */
        void waitUntilRunning() throws InterruptedException {
            isRunningLatch.await();
        }

        @Override
        protected void stop() {
            filteredProxy.deregister();
        }

        /**
         * Indicates if this instance has enough clients.
         * 
         * @return {@code true} if and only if this instance has enough clients.
         */
        private boolean enoughClients() {
            return getClientCount() >= MINIMUM_NUMBER_OF_CLIENTS_PER_FILTER;
        }

        /**
         * Ranks the clients from worst-performing to best-performing.
         */
        private synchronized void rankClients() {
            rankedClients.clear();
            for (final Client client : clients) {
                final RankedClient rankedClient = new RankedClient(client);
                rankedClients.add(rankedClient);
            }
        }

        /**
         * Removes the worst-performing client.
         */
        private synchronized void removeWorstClient() {
            if (!rankedClients.isEmpty()) {
                final RankedClient rankedClient = rankedClients.first();
                rankedClients.remove(rankedClient);
                final Client client = rankedClient.client;
                client.cancel();
                clients.remove(client);
            }
        }

        /**
         * Adds a new client.
         * <p>
         * This method is potentially uninterruptible and slow.
         * 
         * @return {@code true} if and only if a new client was successfully
         *         added.
         * @throws NoSuchFileException
         *             if the tracker was not helpful and no tracker-specific
         *             topology-file exists in the archive.
         * @throws SocketException
         *             if {@link #stop()} is called by another thread
         * @throws IOException
         *             if an I/O error occurs.
         */
        private boolean addClient() throws NoSuchFileException, IOException {
            boolean clientAdded = false;
            final Topology topology = filteredProxy.getTopology();
            final InetSocketAddress remoteServer = computeBestServer(topology);

            if (remoteServer != null) {
                final Client client = new Client(localServer, remoteServer,
                        filter, clearingHouse);
                synchronized (this) {
                    clients.add(client);
                }
                clientCompletionService.submit(new ClientWrapper(client));
                clientAdded = true;
            }
            return clientAdded;
        }

        /**
         * Returns information on the best server to connect to next.
         * 
         * @param topology
         *            The current state of the network. May be modified by this
         *            method.
         * @return Address of the next server to connect to or {@code null} if
         *         no such server exists.
         */
        private InetSocketAddress computeBestServer(final Topology topology) {
            /*
             * Remove servers from the network that should not be considered.
             */
            synchronized (this) {
                for (final Client client : clients) {
                    topology.remove(client.getServerAddress());
                }
                topology.remove(invalidServers);
                final Collection<Peer> extantPeers = clearingHouse
                        .getPeers(filter);
                for (final Peer peer : extantPeers) {
                    topology.remove(peer.getRemoteServerSocketAddress());
                }
            }
            topology.remove(localServer);
            final InetSocketAddress bestServer = topology.getBestServer(filter);
            logger.debug("Best server is {}", bestServer);
            return bestServer;
        }

        /**
         * Waits until done or a timeout occurs or (optionally) a new client is
         * needed.
         * <p>
         * This operation is potentially slow.
         * 
         * @param returnIfNeedClient
         *            Whether or not to return if {@link #enoughClients()} is
         *            false.
         * @param timeout
         *            Timeout in seconds
         * @return The timeout for the next time
         * @throws InterruptedException
         *             if the current thread is interrupted.
         */
        private synchronized int waitUntilDoneOrTimeout(
                final boolean returnIfNeedClient, final int timeout)
                throws InterruptedException {
            long delay = 1000 * timeout;
            while (!Thread.currentThread().isInterrupted()
                    && (!returnIfNeedClient || enoughClients()) && delay > 0) {
                final long start = System.currentTimeMillis();
                wait(delay); // notified by terminating client and stop()
                delay -= (System.currentTimeMillis() - start);
            }
            return (delay > 0)
                    ? 0
                    : Math.min(Math.max(2 * timeout, 1), REPLACEMENT_PERIOD);
        }

        /**
         * Restarts the client counters.
         */
        private synchronized void restartClientCounters() {
            for (final Client client : clients) {
                client.restartCounter();
            }
        }
    }

    /**
     * Reaps completed clients.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private final class ClientReaper implements Callable<Void> {
        /**
         * The "isRunning" latch.
         */
        private final CountDownLatch isRunningLatch = new CountDownLatch(1);

        /**
         * Reaps clients. Returns if and only if all desired data has been
         * received.
         * 
         * @throws InterruptedException
         *             if the current thread is interrupted
         * @throws IOException
         *             if a non-networking error occurs
         */
        @Override
        public Void call() throws IOException, InterruptedException {
            isRunningLatch.countDown();
            for (;;) {
                final Future<Boolean> clientFuture = clientCompletionService
                        .take();
                if (clientFuture.isCancelled()) {
                    throw new InterruptedException();
                }
                try {
                    if (clientFuture.get()) {
                        // All desired-data received
                        return null;
                    }
                }
                catch (final ExecutionException e) {
                    final Throwable cause = e.getCause();
                    if (cause instanceof IOException) {
                        throw (IOException) cause;
                    }
                    throw new RuntimeException("Unexpected error", cause);
                }
            }
        }

        /**
         * Waits until this instance is running.
         * <p>
         * This method is potentially slow.
         * 
         * @throws InterruptedException
         *             if the current thread is interrupted
         */
        void waitUntilRunning() throws InterruptedException {
            isRunningLatch.await();
        }
    }

    /**
     * A ranked client. The natural order of this class is from poorer
     * performing clients to higher performing ones.
     * <p>
     * Instances are Immutable.
     * 
     * @author Steven R. Emmerson
     */
    @Immutable
    private static final class RankedClient implements Comparable<RankedClient> {
        /**
         * The client in question.
         */
        private final Client client;
        /**
         * The ranking.
         */
        private final long   rank;

        /**
         * Constructs from a client.
         * 
         * @param client
         *            The client.
         */
        RankedClient(final Client client) {
            this.client = client;
            // TODO: implement and use Client.getRate();
            this.rank = client.getCounter();
        }

        @Override
        public int compareTo(final RankedClient that) {
            return rank < that.rank
                    ? -1
                    : rank == that.rank
                            ? 0
                            : 1;
        }
    }

    /**
     * Wraps a {@link Client} in order to perform client-dependent actions when
     * the client completes.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private final class ClientWrapper extends UninterruptibleTask<Boolean> {
        /**
         * The client.
         */
        private final Client client;

        /**
         * Constructs from the {@link Client} to be executed.
         * 
         * @param client
         *            the {@link Client} to be executed.
         */
        ClientWrapper(final Client client) {
            this.client = client;
        }

        /**
         * @return {@code true} if and only if all desired-data has been
         *         received.
         * @throws IOException
         *             if an I/O error occurred that is unrelated to networking
         */
        @Override
        public Boolean call() throws IOException {
            boolean allDataReceived = false;
            boolean reportOffline = true;
            try {
                final boolean validServer = client.call();
                reportOffline = false;
                if (validServer) {
                    logger.debug("All desired-data received");
                    allDataReceived = true;
                }
                else {
                    synchronized (ClientManager.this) {
                        // TODO: slowly remove "invalidServers" entries
                        invalidServers.add(client.getServerAddress());
                    }
                }
            }
            catch (final InterruptedException e) {
                logger.trace("Interrupted: {}", client);
            }
            catch (final EOFException e) {
                logger.info("Connection closed by remote server: {}: {}",
                        client.getServerAddress(), e.toString());
            }
            catch (final ConnectException e) {
                logger.info("Couldn't connect to remote server: {}: {}",
                        client.getServerAddress(), e.toString());
            }
            catch (final SocketException e) {
                logger.info("Remote server is inaccessible: {}: {}",
                        client.getServerAddress(), e.toString());
            }
            catch (final IOException e) {
                reportOffline = false;
                throw new IOException("Client I/O failure: " + client, e);
            }
            catch (final Throwable t) {
                reportOffline = false;
                throw new RuntimeException("Unexpected error: " + client, t);
            }
            finally {
                if (reportOffline) {
                    final InetSocketAddress remoteServerAddress = client
                            .getServerAddress();
                    try {
                        trackerProxy.reportOffline(remoteServerAddress);
                    }
                    catch (final IOException e) {
                        logger.warn("Couldn't report {} as being offline: {}",
                                remoteServerAddress, e.toString());
                    }
                }
                synchronized (ClientManager.this) {
                    clients.remove(client);
                    ClientManager.this.notifyAll();
                }
            }
            return allDataReceived;
        }

        @Override
        protected void stop() {
            logger.trace("Stop: {}", client);
            client.cancel();
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + " [client=" + client + "]";
        }
    }

    /**
     * The logger for this class.
     */
    private static Logger                      logger                                       = Util.getLogger();
    /**
     * The minimum number of servers per filter.
     */
    private static final int                   MINIMUM_NUMBER_OF_CLIENTS_PER_FILTER;
    private static final String                MINIMUM_NUMBER_OF_CLIENTS_PER_FILTER_KEY     = "minimum number of clients per filter";
    private static final int                   MINIMUM_NUMBER_OF_CLIENTS_PER_FILTER_DEFAULT = 8;
    /**
     * The monitoring interval, in seconds, between client replacements.
     */
    private static final int                   REPLACEMENT_PERIOD;
    private static final String                REPLACEMENT_PERIOD_KEY                       = "client replacement period in seconds";
    private static final int                   REPLACEMENT_PERIOD_DEFAULT                   = 60;
    /**
     * The keep-alive time, in seconds, for client threads.
     */
    private static final int                   CLIENT_THREAD_KEEP_ALIVE_TIME;
    private static final String                CLIENT_THREAD_KEEP_ALIVE_TIME_KEY            = "client thread keep-alive time in seconds";
    private static final int                   CLIENT_THREAD_KEEP_ALIVE_TIME_DEFAULT        = 60;

    static {
        final Preferences prefs = Preferences
                .userNodeForPackage(SinkNode.class);
        MINIMUM_NUMBER_OF_CLIENTS_PER_FILTER = prefs.getInt(
                MINIMUM_NUMBER_OF_CLIENTS_PER_FILTER_KEY,
                MINIMUM_NUMBER_OF_CLIENTS_PER_FILTER_DEFAULT);
        if (MINIMUM_NUMBER_OF_CLIENTS_PER_FILTER <= 0) {
            throw new IllegalArgumentException("Invalid preference: \""
                    + MINIMUM_NUMBER_OF_CLIENTS_PER_FILTER_KEY + "\"="
                    + MINIMUM_NUMBER_OF_CLIENTS_PER_FILTER);
        }
        REPLACEMENT_PERIOD = prefs.getInt(REPLACEMENT_PERIOD_KEY,
                REPLACEMENT_PERIOD_DEFAULT);
        if (REPLACEMENT_PERIOD <= 0) {
            throw new IllegalArgumentException("Invalid preference: \""
                    + REPLACEMENT_PERIOD_KEY + "\"=" + REPLACEMENT_PERIOD);
        }
        CLIENT_THREAD_KEEP_ALIVE_TIME = prefs.getInt(
                CLIENT_THREAD_KEEP_ALIVE_TIME_KEY,
                CLIENT_THREAD_KEEP_ALIVE_TIME_DEFAULT);
        if (CLIENT_THREAD_KEEP_ALIVE_TIME < 0) {
            throw new IllegalArgumentException("Invalid preference: \""
                    + CLIENT_THREAD_KEEP_ALIVE_TIME_KEY + "\"="
                    + CLIENT_THREAD_KEEP_ALIVE_TIME);
        }
    }

    /**
     * The proxy for the tracker.
     */
    private final TrackerProxy                 trackerProxy;
    /**
     * The task-execution service.
     */
    private final CancellingExecutor           executor                                     = new CancellingExecutor(
                                                                                                    0,
                                                                                                    Integer.MAX_VALUE,
                                                                                                    CLIENT_THREAD_KEEP_ALIVE_TIME,
                                                                                                    TimeUnit.SECONDS,
                                                                                                    new SynchronousQueue<Runnable>());
    /**
     * The task-completion service
     */
    private final CompletionService<Void>      completionService                            = new ExecutorCompletionService<Void>(
                                                                                                    executor);
    /**
     * The client-completion service
     */
    private final CompletionService<Boolean>   clientCompletionService                      = new ExecutorCompletionService<Boolean>(
                                                                                                    executor);
    /**
     * The clients being managed by this instance.
     */
    @GuardedBy("this")
    private final List<Client>                 clients                                      = new LinkedList<Client>();
    /**
     * The set of offline servers.
     */
    @GuardedBy("this")
    private final SortedSet<InetSocketAddress> invalidServers                               = new TreeSet<InetSocketAddress>(
                                                                                                    AddressComparator.INSTANCE);
    /**
     * The set of ranked clients.
     */
    @GuardedBy("this")
    private final SortedSet<RankedClient>      rankedClients                                = new TreeSet<RankedClient>();
    /**
     * The data clearing-house.
     */
    private final ClearingHouse                clearingHouse;
    /**
     * The specification of locally-desired data.
     */
    private final Filter                       filter;
    /**
     * Address of the local server.
     */
    private final InetSocketAddress            localServer;
    /**
     * The creator of new clients
     */
    private final ClientCreator                clientCreator;
    /**
     * The reaper of completed clients
     */
    private final ClientReaper                 clientReaper;

    /**
     * Constructs from the data clearing house, the address of the local server,
     * the filter to use, and the address of the tracker.
     * 
     * @param localServer
     *            The node identifier.
     * @param clearingHouse
     *            The data clearing house.
     * @param comparableInetSocketAddress
     *            The address of the local server.
     * @param filter
     *            The data filter to use.
     * @param trackerProxy
     *            The proxy for the tracker.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code localServer == null}.
     * @throws NullPointerException
     *             if {@code clearingHouse == null}.
     * @throws NullPointerException
     *             if {@code serverSocketAddress == null}.
     * @throws NullPointerException
     *             if {@code trackerProxy == null}.
     */
    ClientManager(final InetSocketAddress localServer,
            final ClearingHouse clearingHouse, final Filter filter,
            final TrackerProxy trackerProxy) throws IOException {
        if (localServer == null) {
            throw new NullPointerException();
        }
        if (clearingHouse == null) {
            throw new NullPointerException();
        }
        if (trackerProxy == null) {
            throw new NullPointerException();
        }
        this.trackerProxy = trackerProxy;
        this.localServer = localServer;
        this.filter = filter;
        this.clearingHouse = clearingHouse;
        this.clientCreator = new ClientCreator();
        this.clientReaper = new ClientReaper();
    }

    /**
     * Returns this instance's {@link Filter}.
     * 
     * @return this instance's {@link Filter}.
     */
    public Filter getFilter() {
        return filter;
    }

    /**
     * Returns the Internet socket address of the local server.
     * 
     * @return the Internet socket address of the local server
     */
    InetSocketAddress getLocalServerAddress() {
        return localServer;
    }

    /**
     * Runs this instance. Returns if and only if all desired-data was received.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted
     * @throws IOException
     *             if an I/O error that is not related to networking occurs
     */
    public Void call() throws InterruptedException, IOException {
        logger.trace("Starting up: {}", this);
        final String prevName = Thread.currentThread().getName();
        Thread.currentThread().setName(toString());
        try {
            final Future<Void> clientCreatorFuture = completionService
                    .submit(clientCreator);
            final Future<Void> clientReaperFuture = completionService
                    .submit(clientReaper);
            final Future<Void> future = completionService.take();
            if (future == clientCreatorFuture) {
                /*
                 * The client-creation task completed. Ideally because the
                 * thread on which it was executing was interrupted.
                 */
                if (clientCreatorFuture.isCancelled()) {
                    throw new InterruptedException();
                }
                try {
                    clientCreatorFuture.get();
                    throw new AssertionError();
                }
                catch (final ExecutionException e) {
                    final Throwable cause = e.getCause();
                    if (cause instanceof InterruptedException) {
                        throw (InterruptedException) cause;
                    }
                    throw new RuntimeException("Client creator crashed: "
                            + this, cause);
                }
            }
            else {
                /*
                 * The client-reaping task completed. Ideally because all
                 * desired-data was received.
                 */
                if (clientReaperFuture.isCancelled()) {
                    throw new InterruptedException();
                }
                try {
                    clientReaperFuture.get();
                }
                catch (final ExecutionException e) {
                    final Throwable cause = e.getCause();
                    if (cause instanceof IOException) {
                        throw (IOException) cause;
                    }
                    throw new RuntimeException("Client crashed", cause);
                }
            }
        }
        finally {
            executor.shutdownNow();
            awaitCompletion();
            Thread.currentThread().setName(prevName);
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
     *             if the current thread is interrupted
     */
    void waitUntilRunning() throws InterruptedException {
        clientCreator.waitUntilRunning();
        clientReaper.waitUntilRunning();
    }

    /**
     * Waits until this instance has completed.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted while waiting
     */
    void awaitCompletion() throws InterruptedException {
        Thread.interrupted();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }

    /**
     * Returns the number of active clients.
     * 
     * @return the number of active clients.
     */
    synchronized int getClientCount() {
        return clients.size();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "ClientManager [trackerProxy=" + trackerProxy + ", clients=("
                + getClientCount() + ")]";
    }
}
