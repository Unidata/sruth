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
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.prefs.Preferences;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;

import edu.ucar.unidata.sruth.Archive.DistributedTrackerFiles;

/**
 * Manages a set of clients. Populates the set, removes poor performing members,
 * and adds new members as appropriate.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class ClientManager extends UninterruptibleTask<Void> {
    /**
     * The logger for this class.
     */
    private static Logger logger = Util.getLogger();

    /**
     * A ranking of a client. The natural order of this class is from poorer
     * performing clients to higher performing ones.
     * <p>
     * Instances are Immutable.
     * 
     * @author Steven R. Emmerson
     */
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
     * The proxy for the tracker.
     */
    private final TrackerProxy                 trackerProxy;
    /**
     * The task execution service.
     */
    private final CancellingExecutor           clientExecutor                               = new CancellingExecutor(
                                                                                                    0,
                                                                                                    Integer.MAX_VALUE,
                                                                                                    CLIENT_THREAD_KEEP_ALIVE_TIME,
                                                                                                    TimeUnit.SECONDS,
                                                                                                    new SynchronousQueue<Runnable>());
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
     * @param trackerAddress
     *            The address of the tracker.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code localServer == null}.
     * @throws NullPointerException
     *             if {@code clearingHouse == null}.
     * @throws NullPointerException
     *             if {@code serverSocketAddress == null}.
     * @throws NullPointerException
     *             if {@code filter == null}.
     * @throws NullPointerException
     *             if {@code trackerAddress == null}.
     */
    ClientManager(final InetSocketAddress localServer,
            final ClearingHouse clearingHouse, final Filter filter,
            final InetSocketAddress trackerAddress) throws IOException {
        if (localServer == null) {
            throw new NullPointerException();
        }
        if (clearingHouse == null) {
            throw new NullPointerException();
        }
        final DistributedTrackerFiles distributedTrackerFiles = clearingHouse
                .getDistributedTrackerFiles(trackerAddress);
        trackerProxy = new TrackerProxy(trackerAddress, filter, localServer,
                distributedTrackerFiles);
        this.localServer = localServer;
        this.filter = filter;
        this.clearingHouse = clearingHouse;
    }

    /**
     * Runs this instance. Returns normally if and only if all desired data has
     * been received or {@link #stop()} has been called.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws NoSuchFileException
     *             if the tracker couldn't be contacted and there's no
     *             tracker-specific topology-file in the archive.
     */
    public Void call() throws NoSuchFileException, InterruptedException {
        final String prevName = Thread.currentThread().getName();
        Thread.currentThread().setName(toString());
        try {
            while (!isCancelled()) {
                if (enoughClients()) {
                    rankClients();
                    do {
                        removeWorstClient();
                    } while (!isCancelled() && enoughClients());
                }
                while (!isCancelled() && !enoughClients()) {
                    try {
                        if (!addClient()) {
                            waitUntilDoneOrTimeout(false);
                        }
                    }
                    catch (final NoSuchFileException e) {
                        throw e;
                    }
                    catch (final Exception e) {
                        logger.warn("Couldn't add new client: ", e.toString());
                        waitUntilDoneOrTimeout(false);
                    }
                }
                restartClientCounters();
                waitUntilDoneOrTimeout(true);
            }
        }
        finally {
            trackerProxy.close();
            clientExecutor.shutdownNow();
            Thread.currentThread().setName(prevName);
        }
        return null;
    }

    /**
     * Stops this instance.
     */
    @Override
    protected synchronized void stop() {
        logger.trace("stop() called");
        trackerProxy.close();
        clientExecutor.shutdownNow();
        notifyAll();
    }

    /**
     * Indicates if this instance has enough clients.
     * 
     * @return {@code true} if and only if this instance has enough clients.
     */
    private synchronized boolean enoughClients() {
        return clients.size() >= MINIMUM_NUMBER_OF_CLIENTS_PER_FILTER;
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
     * @return {@code true} if and only if a new client was successfully added.
     * @throws ClassCastException
     *             if the tracker returns the wrong type.
     * @throws ClassNotFoundException
     *             if the tracker's reply is invalid.
     * @throws NoSuchFileException
     *             if the tracker couldn't be contacted and no tracker-specific
     *             topology-file exists in the archive.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private boolean addClient() throws ClassNotFoundException,
            NoSuchFileException, IOException {
        boolean clientAdded = false;
        FilterServerMap network = trackerProxy.getNetwork(clients.size() == 0);
        network = new FilterServerMap(network);
        final InetSocketAddress remoteServer = computeBestServer(network);

        if (remoteServer != null) {
            final Client client = new Client(localServer, remoteServer, filter,
                    clearingHouse);
            synchronized (this) {
                clients.add(client);
                clientExecutor.submit(new UninterruptibleTask<Void>() {
                    @Override
                    public Void call() {
                        boolean validServer = false;
                        boolean reportOffline = false;
                        try {
                            validServer = client.call().booleanValue();
                            if (!validServer) {
                                logger.debug("Invalid server: {}", client);
                            }
                            else {
                                logger.debug("Client returned normally: {}",
                                        client);
                                ClientManager.this.cancel(); // because all
                                                             // desired-data
                                                             // received
                            }
                        }
                        catch (final InterruptedException e) {
                            logger.debug("Client was interrupted: {}",
                                    e.toString());
                        }
                        catch (final EOFException e) {
                            logger.info(
                                    "Connection closed by remote server: {}: {}",
                                    remoteServer, e.toString());
                            reportOffline = true;
                        }
                        catch (final ConnectException e) {
                            if (isCancelled()) {
                                logger.debug("Client was cancelled: {}",
                                        e.toString());
                            }
                            else {
                                logger.info(
                                        "Couldn't connect to remote server: {}: {}",
                                        remoteServer, e.toString());
                                reportOffline = true;
                            }
                        }
                        catch (final SocketException e) {
                            if (isCancelled()) {
                                logger.debug(
                                        "Client's connection was disconnected: {}",
                                        e.toString());
                            }
                            else {
                                logger.info(
                                        "Connection to remote server closed: {}: {}",
                                        remoteServer, e.toString());
                                reportOffline = true;
                            }
                        }
                        catch (final IOException e) {
                            if (isCancelled()) {
                                logger.debug("Client I/O failure: {}: {}",
                                        client, e.toString());
                            }
                            else {
                                logger.error("Client I/O failure: " + client, e);
                            }
                        }
                        catch (final Throwable t) {
                            logger.warn("Unexpected client failure", t);
                        }
                        finally {
                            synchronized (ClientManager.this) {
                                if (!validServer) {
                                    // TODO: slowly remove entries from
                                    // "invalidServers"
                                    invalidServers.add(remoteServer);
                                }
                                clients.remove(client);
                                ClientManager.this.notifyAll();
                            }
                            if (reportOffline) {
                                try {
                                    trackerProxy.reportOffline(remoteServer);
                                }
                                catch (final IOException e) {
                                    logger.warn(
                                            "Couldn't report {} as being offline: {}",
                                            remoteServer, e.toString());
                                }
                            }
                        }
                        return null;
                    }

                    @Override
                    protected void stop() {
                        logger.trace("stop() called");
                        client.cancel();
                    }
                });
            }
            clientAdded = true;
        }
        return clientAdded;
    }

    /**
     * Returns the number of active clients.
     * 
     * @return the number of active clients.
     */
    synchronized int getClientCount() {
        return clients.size();
    }

    /**
     * Returns information on the best server to connect to next.
     * 
     * @param network
     *            The current state of the network. May be modified by this
     *            method.
     * @return Address of the next server to connect to or {@code null} if no
     *         such server exists.
     */
    private InetSocketAddress computeBestServer(final FilterServerMap network) {
        /*
         * Remove servers from the network that should not be considered.
         */
        synchronized (this) {
            for (final Client client : clients) {
                network.remove(client.getServerAddress());
            }
            network.remove(invalidServers);
            final Collection<Peer> extantPeers = clearingHouse.getPeers(filter);
            for (final Peer peer : extantPeers) {
                network.remove(peer.getRemoteServerSocketAddress());
            }
        }
        network.remove(localServer);
        final InetSocketAddress bestServer = network.getBestServer(filter);
        logger.debug("Best server is {}", bestServer);
        return bestServer;
    }

    /**
     * Restarts the client counters.
     */
    private synchronized void restartClientCounters() {
        for (final Client client : clients) {
            client.restartCounter();
        }
    }

    /**
     * Waits until done or a timeout occurs or (optionally) a new client is
     * needed.
     * <p>
     * This operation is potentially slow.
     * 
     * @param returnIfNeedClient
     *            Whether or not to return if {@link #enoughClients()} is false.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    private synchronized void waitUntilDoneOrTimeout(
            final boolean returnIfNeedClient) throws InterruptedException {
        long timeout = 1000 * REPLACEMENT_PERIOD;
        while (!isCancelled() && (!returnIfNeedClient || enoughClients())
                && timeout > 0) {
            final long start = System.currentTimeMillis();
            wait(timeout); // notified by terminating client
            timeout -= (System.currentTimeMillis() - start);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "ClientManager [trackerProxy=" + trackerProxy + ", clients=("
                + clients.size() + ")]";
    }
}
