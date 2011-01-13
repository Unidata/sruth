/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.NotThreadSafe;
import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps track of subscriptions by sink-nodes and gives sink-nodes contact
 * information for the source-node and/or other sink-nodes.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class Tracker extends BlockingTask<Void> {
    /**
     * An n-to-m bidirectional mapping between servers and the filters of their
     * data-selection predicates.
     * 
     * Instances are thread-compatible but not thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @NotThreadSafe
    private static final class FilterServerMap {
        /**
         * The map from filters to servers.
         */
        private final Map<Filter, Set<ServerInfo>> serverSets = new HashMap<Filter, Set<ServerInfo>>();
        /**
         * The map from servers to filters.
         */
        private final Map<ServerInfo, Set<Filter>> filterSets = new HashMap<ServerInfo, Set<Filter>>();

        /**
         * Returns the set of servers that can satisfy a given file-selection
         * filter. The set is backed by this instance, so modifying it will
         * modify this instance.
         * 
         * @param filter
         *            The filter to satisfy.
         * @return The set of satisfying servers. May be empty.
         */
        Set<ServerInfo> getServers(final Filter filter) {
            Set<ServerInfo> serverSet = serverSets.get(filter);
            if (serverSet == null) {
                serverSet = Collections.emptySet();
            }
            return serverSet;
        }

        /**
         * Adds to a mapping from a filter to a sink-node's server. Creates the
         * entry if it doesn't already exist.
         * 
         * @param filter
         *            The file-selection filter.
         * @param serverInfo
         *            Information on the sink-node's server.
         */
        void add(final Filter filter, final ServerInfo serverInfo) {
            Set<ServerInfo> serverInfos = serverSets.get(filter);
            if (null == serverInfos) {
                serverInfos = new TreeSet<ServerInfo>();
                serverSets.put(filter, serverInfos);
            }
            serverInfos.add(serverInfo);

            Set<Filter> filters = filterSets.get(serverInfo);
            if (filters == null) {
                filters = new TreeSet<Filter>();
                filterSets.put(serverInfo, filters);
            }
            filters.add(filter);
        }

        /**
         * Removes a sink-node's server.
         * 
         * @param serverInfo
         *            Information on the sink-node's server to be removed.
         */
        void remove(final ServerInfo serverInfo) {
            final Set<Filter> filters = filterSets.get(serverInfo);
            if (filters != null) {
                for (final Filter filter : filters) {
                    final Set<ServerInfo> serverInfos = serverSets.get(filter);
                    if (serverInfos != null) {
                        serverInfos.remove(serverInfo);
                    }
                }
            }
            filterSets.remove(serverInfo);
        }
    }

    /**
     * Handles one and only one sink-node.
     * 
     * Instances are thread-compatible but not thread-safe.
     */
    @NotThreadSafe
    class Trackerlet extends BlockingTask<Void> implements Callable<Void> {
        /**
         * The socket.
         */
        private final Socket socket;

        /**
         * Constructs from a socket.
         * 
         * @param socket
         *            The socket.
         * @throws NullPointerException
         *             if {@code socket == null}.
         */
        Trackerlet(final Socket socket) {
            if (null == socket) {
                throw new NullPointerException();
            }
            this.socket = socket;
        }

        /**
         * If an error occurs (e.g., the object input stream couldn't be
         * initialized), then that fact is logged. The connection is closed upon
         * completion.
         * 
         * @throws IOException
         *             if an I/O error occurs.
         * @throws ClassNotFoundException
         *             if the class of the incoming object can't be found.
         */
        @Override
        public Void call() throws IOException, ClassNotFoundException {
            // TODO: set time-limit interruption
            final String origThreadName = Thread.currentThread().getName();
            Thread.currentThread().setName(toString());
            try {
                final InputStream inputStream = socket.getInputStream();
                final ObjectInputStream ois = new ObjectInputStream(inputStream);
                try {
                    final TrackerTask trackerTask = (TrackerTask) ois
                            .readObject();
                    trackerTask.process(Tracker.this, socket);
                }
                finally {
                    try {
                        ois.close();
                    }
                    catch (final IOException ignored) {
                    }
                }
            }
            catch (final IOException e) {
                logger.warn("I/O error on socket {}: {}", socket, e);
                throw e;
            }
            catch (final ClassNotFoundException e) {
                logger.warn("Unknown request on socket {}: {}", socket, e);
                throw e;
            }
            catch (final Throwable t) {
                logger.error("Unexpected error on socket {}: {}", socket, t);
                throw Util.launderThrowable(t);
            }
            finally {
                Thread.currentThread().setName(origThreadName);
                try {
                    socket.close();
                }
                catch (final IOException ignored) {
                }
            }
            return null;
        }

        @Override
        protected void stop() {
            try {
                socket.close();
            }
            catch (final IOException ignored) {
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return "Trackerlet [socket=" + socket + "]";
        }
    }

    /**
     * The logger for this class.
     */
    private static final Logger   logger              = LoggerFactory
                                                              .getLogger(Tracker.class);
    /**
     * Executes each task for a single sink-node.
     */
    private final ExecutorService executorService     = new CancellingExecutor(
                                                              0,
                                                              25,
                                                              60,
                                                              TimeUnit.SECONDS,
                                                              new SynchronousQueue<Runnable>());
    /**
     * The socket on which this instance listens.
     */
    private final ServerSocket    serverSocket;
    /**
     * The filter-to-servers map.
     */
    @GuardedBy("this")
    private final FilterServerMap filterServerMapping = new FilterServerMap();
    /**
     * Information on the source-server.
     */
    private final ServerInfo      sourceServer;
    /**
     * The set of directly-connected clients.
     */
    @GuardedBy("this")
    private final Set<ServerInfo> directClients       = new HashSet<ServerInfo>();
    /**
     * The maximum number of directly-connected clients.
     */
    private final int             directClientLimit;

    /**
     * Constructs from information on the source-server.
     * 
     * @param portSet
     *            The set of candidate port numbers.
     * @param sourceServer
     *            Information on the source-server.
     * @param directClientLimit
     *            The maximum number of directly-connected clients.
     * @throws IllegalArgumentException
     *             if {@code directClientLimit <= 0};
     * @throws IOException
     *             if a server socket can't be created.
     * @throws NullPointerException
     *             if {@code sourceServer == null || portSet == null}.
     */
    Tracker(final PortNumberSet portSet, final ServerInfo sourceServer,
            final int directClientLimit) throws IOException {
        if (null == sourceServer) {
            throw new NullPointerException();
        }
        if (directClientLimit <= 0) {
            throw new IllegalArgumentException(
                    "Invalid directly-connected client limit: "
                            + directClientLimit);
        }
        this.directClientLimit = directClientLimit;
        serverSocket = new ServerSocket();
        try {
            for (final int port : portSet) {
                try {
                    serverSocket.bind(new InetSocketAddress(port));
                    break;
                }
                catch (final IOException ignored) {
                }
            }
            if (serverSocket.isBound()) {
                this.sourceServer = sourceServer;
                return;
            }
            else {
                throw new IOException(
                        "Couldn't find unused port for tracker in " + portSet);
            }
        }
        catch (final IOException e) {
            try {
                serverSocket.close();
            }
            catch (final Exception ignored) {
            }
            throw e;
        }
    }

    /**
     * Returns the local address of the this instance's server socket.
     * 
     * @return The local address of the this instance's server socket.
     */
    InetSocketAddress getAddress() {
        return (InetSocketAddress) serverSocket.getLocalSocketAddress();
    }

    /**
     * Executes this instance. Completes normally if and only if the current
     * thread is interrupted. Closes the tracker socket.
     * 
     * @throws IOException
     *             if an I/O error occurs.
     */
    public Void call() throws IOException {
        final String origThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(toString());
        try {
            for (;;) {
                try {
                    // TODO: limit number of outstanding tasks
                    final Socket socket = serverSocket.accept();
                    final Trackerlet trackerlet = new Trackerlet(socket);
                    executorService.submit(trackerlet);
                }
                catch (final IOException e) {
                    if (!Thread.currentThread().isInterrupted()) {
                        throw e;
                    }
                    // Implements thread interruption policy
                    return null;
                }
            }
        }
        finally {
            executorService.shutdownNow();
            try {
                serverSocket.close();
            }
            catch (final IOException ignored) {
            }
            Thread.currentThread().setName(origThreadName);
        }
    }

    /**
     * Handles the disconnection of a sink-node.
     * 
     * @param serverInfo
     *            Information on the sink-node's server.
     */
    synchronized void clientDisconnected(final ServerInfo serverInfo) {
        directClients.remove(serverInfo);
        filterServerMapping.remove(serverInfo);
    }

    /**
     * Handles a report of a sink-node's server going offline.
     * 
     * @param serverInfo
     *            Information on the sink-node's server.
     */
    synchronized void serverOffline(final ServerInfo serverInfo) {
        // TODO: could be from malware => vet source and verify
        // unavailability of sink-node's server
        filterServerMapping.remove(serverInfo);
    }

    @Override
    protected void stop() {
        executorService.shutdownNow();
        try {
            serverSocket.close();
        }
        catch (final IOException ignored) {
        }
    }

    /**
     * Tells a {@link TrackerPlumber} about the servers that can satisfy its sink-node
     * predicate.
     * 
     * @param predicate
     *            The file-selection predicate to satisfy.
     * @param sinkServerInfo
     *            Information on the sink-node's server.
     * @param connector
     *            The object that will connect the sink-node to the servers that
     *            can satisfy its predicate.
     * @return A object that will appropriately connect a sink-node.
     */
    synchronized void informConnector(final Predicate predicate,
            final ServerInfo sinkServerInfo, final Connector connector) {
        for (final Filter filter : predicate) {
            final Set<ServerInfo> servers = filterServerMapping
                    .getServers(filter);
            for (final ServerInfo serverInfo : servers) {
                // Only consider servers that aren't the sink-node's server
                if (!serverInfo.equals(sinkServerInfo)) {
                    connector.add(serverInfo, filter);
                }
            }
        }
        if (directClients.size() < directClientLimit) {
            connector.put(sourceServer, predicate);
            directClients.add(sinkServerInfo);
        }
        register(sinkServerInfo, predicate);
    }

    /**
     * Registers a server capable of satisfying a given file-selection
     * predicate.
     * 
     * @param serverInfo
     *            Information on the sink-node's server.
     * @param predicate
     *            The file-selection predicate.
     * @throws NullPointerException
     *             if {@code serverInfo == null || predicate == null}.
     */
    private void register(final ServerInfo serverInfo, final Predicate predicate) {
        assert Thread.holdsLock(this);
        for (final Filter filter : predicate) {
            filterServerMapping.add(filter, serverInfo);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Tracker [serverSocket=" + serverSocket + ", sourceServer="
                + sourceServer + "]";
    }
}
