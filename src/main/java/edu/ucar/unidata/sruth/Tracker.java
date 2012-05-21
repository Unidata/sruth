/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.BindException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.prefs.Preferences;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.NotThreadSafe;
import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;

/**
 * Keeps track of subscriptions by sink-nodes and gives sink-nodes contact
 * information for the source-node and/or other sink-nodes.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class Tracker implements Callable<Void> {
    /**
     * Checks the status of servers.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private class ServerCheckerTask extends UninterruptibleTask<Void> {
        /**
         * Checks the status of a server (i.e., whether it's online or offline).
         * <p>
         * Instances are thread-safe.
         * 
         * @author Steven R. Emmerson
         */
        private class ServerChecker extends UninterruptibleTask<Void> {
            /**
             * The address of the server.
             */
            private final InetSocketAddress  serverAddress;
            /**
             * The connection to the server.
             */
            @GuardedBy("this")
            private final ConnectionToServer connection;

            /**
             * Constructs from the address of the server.
             * 
             * @param serverAddress
             *            The address of the server.
             * @throws NullPointerException
             *             if {@code serverAddress == null}.
             */
            ServerChecker(final InetSocketAddress serverAddress) {
                if (serverAddress == null) {
                    throw new NullPointerException();
                }
                this.serverAddress = serverAddress;
                connection = new ConnectionToServer(sourceServer, serverAddress);
            }

            /**
             * Checks whether the server is online or offline by attempting to
             * connect to it. Removes the server from consideration by the
             * tracker if the server is offline.
             * 
             * @see java.util.concurrent.Callable#call()
             */
            @Override
            public Void call() {
                logger.trace("Starting up: {}", this);
                final String origThreadName = Thread.currentThread().getName();
                Thread.currentThread().setName("Server-checker");
                try {
                    connection.open();
                    logger.debug("Connection to server succeeded: {}",
                            serverAddress);
                    connection.close();
                }
                catch (final IOException e) {
                    topology.remove(serverAddress);
                    logger.debug("Removed server: {}", serverAddress);
                }
                finally {
                    Thread.currentThread().setName(origThreadName);
                    logger.trace("Done: {}", this);
                }
                return null;
            }

            @Override
            protected void stop() {
                connection.close();
            }

            /*
             * (non-Javadoc)
             * 
             * @see java.lang.Object#toString()
             */
            @Override
            public String toString() {
                return "ServerChecker [serverAddress=" + serverAddress + "]";
            }
        }

        /**
         * The task execution service.
         */
        private final CancellingExecutor executor = new CancellingExecutor(
                                                          0,
                                                          MAX_NUM_SERVER_CHECKER_THREADS,
                                                          SERVER_CHECKER_KEEPALIVE,
                                                          TimeUnit.SECONDS,
                                                          new SynchronousQueue<Runnable>());
        /**
         * The UDP socket on which this instance listens.
         */
        private final DatagramSocket     socket;

        /**
         * Constructs from nothing. Opens a UDP socket.
         * 
         * @throws SocketException
         *             if a UDP socket couldn't be created.
         */
        ServerCheckerTask() throws SocketException {
            /*
             * Create an Internet socket address that uses the tracker's
             * Internet address but an ephemeral port number.
             */
            final InetSocketAddress reportingAddress = new InetSocketAddress(
                    getServerAddress().getAddress(), 0);
            /*
             * Create a UDP socket.
             */
            socket = new DatagramSocket(reportingAddress);
        }

        /**
         * Returns the Internet socket address on which this instance listens.
         * 
         * @return The Internet socket address
         */
        InetSocketAddress getInetSocketAddress() {
            return (InetSocketAddress) socket.getLocalSocketAddress();
        }

        @Override
        public Void call() throws IOException {
            logger.trace("Starting up: {}", this);
            final String origThreadName = Thread.currentThread().getName();
            Thread.currentThread().setName(this.toString());
            final byte[] packetBuf = new byte[2048];
            final DatagramPacket packet = new DatagramPacket(packetBuf,
                    packetBuf.length);
            try {
                for (;;) {
                    packet.setData(packetBuf);
                    socket.receive(packet);
                    final int length = packet.getLength();
                    if (length > packetBuf.length) {
                        logger.error("Incoming datagram from "
                                + packet.getAddress() + " overan buffer: "
                                + length + " > " + packetBuf.length);
                    }
                    else {
                        try {
                            final InetSocketAddress serverAddress = (InetSocketAddress) Util
                                    .deserialize(packetBuf, 0, length);
                            logger.debug("Server reported offline: {}",
                                    serverAddress);
                            final ServerChecker serverChecker = new ServerChecker(
                                    serverAddress);
                            try {
                                executor.submit(serverChecker);
                            }
                            catch (final RejectedExecutionException e) {
                                logger.debug(
                                        "Couldn't submit server-checker: {}: {}",
                                        serverAddress, e.toString());
                            }
                        }
                        catch (final Exception e) {
                            logger.warn("Invalid datagram from {}: {}",
                                    packet.getSocketAddress(), e.toString());
                        }
                    }
                }
            }
            catch (final IOException e) {
                if (!isCancelled()) {
                    throw e;
                }
            }
            finally {
                try {
                    socket.close();
                }
                catch (final Exception ignored) {
                }
                Thread.currentThread().setName(origThreadName);
                logger.trace("Done: {}", this);
            }
            return null;
        }

        @Override
        protected void stop() {
            socket.close();
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return "ServerCheckerTask []";
        }
    }

    /**
     * Accepts and handles connections on the tracker socket
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private final class Accepter extends UninterruptibleTask<Void> {
        /**
         * Executes each task for a single sink-node.
         */
        private final CancellingExecutor trackerletExecutor = new CancellingExecutor(
                                                                    0,
                                                                    25,
                                                                    60,
                                                                    TimeUnit.SECONDS,
                                                                    new SynchronousQueue<Runnable>());

        @Override
        public Void call() throws InterruptedException, IOException {
            logger.trace("Starting up: {}", this);
            try {
                Thread.currentThread().setName("Tracker-accepter");
                while (!trackerletExecutor.isShutdown()) {
                    // TODO: limit number of outstanding trackerlets
                    final Socket socket = trackerSocket.accept();
                    final Trackerlet trackerlet = new Trackerlet(socket);
                    try {
                        trackerletExecutor.submit(trackerlet);
                    }
                    catch (final Throwable e) {
                        try {
                            socket.close();
                        }
                        catch (final IOException ignored) {
                        }
                        if (!trackerletExecutor.isShutdown()) {
                            throw new RuntimeException("Unexpected error", e);
                        }
                    }
                }
            }
            finally {
                trackerletExecutor.shutdownNow();
                Thread.interrupted();
                trackerletExecutor.awaitTermination(Long.MAX_VALUE,
                        TimeUnit.DAYS);
                logger.trace("Done: {}", this);
            }
            return null;
        }

        @Override
        protected void stop() {
            trackerletExecutor.shutdownNow();
            try {
                trackerSocket.close();
            }
            catch (final IOException e) {
                if (!trackerSocket.isClosed()) {
                    logger.error("Couldn't close tracker's server-socket", e);
                }
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return "Accepter []";
        }
    }

    /**
     * The maximum number of server-checker threads.
     */
    private static final int    MAX_NUM_SERVER_CHECKER_THREADS;
    private static final String MAX_NUM_SERVER_CHECKER_THREADS_KEY     = "maximum number of server-checker threads";
    private static final int    MAX_NUM_SERVER_CHECKER_THREADS_DEFAULT = 16;
    /**
     * The keepalive interval for all server-checker threads in milliseconds.
     */
    private static final long   SERVER_CHECKER_KEEPALIVE;
    private static final String SERVER_CHECKER_KEEPALIVE_KEY           = "server-checker thread keepalive-time in seconds";
    private static final long   SERVER_CHECKER_KEEPALIVE_DEFAULT       = 60;

    static {
        final Preferences prefs = Preferences.userNodeForPackage(Tracker.class);
        MAX_NUM_SERVER_CHECKER_THREADS = prefs.getInt(
                MAX_NUM_SERVER_CHECKER_THREADS_KEY,
                MAX_NUM_SERVER_CHECKER_THREADS_DEFAULT);
        if (MAX_NUM_SERVER_CHECKER_THREADS <= 0) {
            throw new IllegalArgumentException("Invalid user preference: \""
                    + MAX_NUM_SERVER_CHECKER_THREADS_KEY + "\"="
                    + MAX_NUM_SERVER_CHECKER_THREADS);
        }
        SERVER_CHECKER_KEEPALIVE = prefs.getLong(SERVER_CHECKER_KEEPALIVE_KEY,
                SERVER_CHECKER_KEEPALIVE_DEFAULT);
        if (SERVER_CHECKER_KEEPALIVE < 0) {
            throw new IllegalArgumentException("Invalid user preference: \""
                    + SERVER_CHECKER_KEEPALIVE_KEY + "\"="
                    + SERVER_CHECKER_KEEPALIVE);
        }
    }

    /**
     * Handles one and only one sink-node.
     * 
     * Instances are thread-compatible but not thread-safe.
     */
    @NotThreadSafe
    class Trackerlet extends UninterruptibleTask<Void> {
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
         * Executes this instance. If an error occurs (e.g., the object input
         * stream couldn't be initialized), then that fact is logged. The
         * connection is closed upon completion.
         * <p>
         * By agreement with {@link Tracker}, this method throws no checked
         * exception: it handles all checked exceptions internally because it's
         * easier that way.
         */
        @Override
        public Void call() {
            final String origThreadName = Thread.currentThread().getName();
            Thread.currentThread().setName(toString());
            try {
                // socket.setSoTimeout(Connection.SO_TIMEOUT);
                socket.setSoLinger(false, 0); // because flush() always called
                socket.setTcpNoDelay(false); // because flush() called when
                                             // appropriate
                socket.setKeepAlive(true);

                final InputStream inputStream = socket.getInputStream();
                final ObjectInputStream ois = new ObjectInputStream(inputStream);
                try {
                    final TrackerTask trackerTask = (TrackerTask) ois
                            .readObject();
                    try {
                        trackerTask.process(Tracker.this, socket);
                    }
                    catch (final IOException e) {
                        if (!isCancelled()) {
                            logger.error("Couldn't process client request: "
                                    + trackerTask, e);
                        }
                    }
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
                if (!isCancelled()) {
                    logger.error("I/O error on {}: {}", socket, e.toString());
                }
            }
            catch (final Throwable t) {
                logger.error("Unexpected error on {}: {}", socket, t.toString());
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
            catch (final IOException e) {
                if (!isCancelled()) {
                    logger.error("Couldn't close trackerlet's server-socket", e);
                }
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
    private static final Logger         logger                         = Util.getLogger();
    /**
     * The name of the network topology property.
     */
    private static final String         NETWORK_TOPOLOGY_PROPERTY_NAME = "Network Topology";
    /**
     * The executor service
     */
    private final CancellingExecutor    executor                       = new CancellingExecutor(
                                                                               2,
                                                                               2,
                                                                               0,
                                                                               TimeUnit.SECONDS,
                                                                               new SynchronousQueue<Runnable>());
    /**
     * The socket on which this instance listens.
     */
    private final ServerSocket          trackerSocket;
    /**
     * The filter/servers map.
     */
    @GuardedBy("this")
    private final Topology       topology                = new Topology();
    /**
     * Information on the source-server.
     */
    private final InetSocketAddress     sourceServer;
    /**
     * Property change support.
     */
    private final PropertyChangeSupport propertySupport;
    /**
     * The "isRunning" latch.
     */
    private final CountDownLatch        isRunningLatch                 = new CountDownLatch(
                                                                               1);
    /**
     * The task that checks whether a server is offline.
     */
    private final ServerCheckerTask     serverCheckerTask;

    /**
     * The IANA-assigned port-number for the tracker
     */
    public static final int             IANA_PORT                      = 38800;

    /**
     * Constructs from information on the source-server. The tracker will listen
     * on its IANA-assigned port and the local host's IP address (as returned by
     * {@link InetAddress#getLocalHost()}).
     * 
     * @param sourceServer
     *            Address of the source-server.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws UnknownHostException
     *             if the Internet address of the local host can't be
     *             determined.
     * @throws NullPointerException
     *             if {@code sourceServer == null}.
     * @see #getServerAddress()
     */
    Tracker(final InetSocketAddress sourceServer) throws UnknownHostException,
            IOException {
        this(sourceServer, new InetSocketAddress(InetAddress.getLocalHost(),
                IANA_PORT));
    }

    /**
     * Constructs from the Internet socket address of the source-server and the
     * Internet socket address for the tracker.
     * 
     * @param sourceServer
     *            The Internet socket address of the source-server.
     * @param trackerSocketAddress
     *            The Internet socket address for the tracker.
     * @throws BindException
     *             if the tracker socket couldn't be bound to the given Internet
     *             socket address
     * @throws SocketException
     *             if the {@code SO_REUSEADDR} option on the tracker socket
     *             couldn't be set
     * @throws IOException
     *             if a socket for the tracker couldn't be created
     * @throws NullPointerException
     *             if {@code sourceServer == null}.
     * @throws NullPointerException
     *             if {@code inetSockAddrSet == null}.
     */
    Tracker(final InetSocketAddress sourceServer,
            final InetSocketAddress trackerSocketAddress) throws BindException,
            SocketException, IOException {
        if (sourceServer == null) {
            throw new NullPointerException();
        }
        if (trackerSocketAddress == null) {
            throw new NullPointerException();
        }
        trackerSocket = new ServerSocket();
        try {
            trackerSocket.setReuseAddress(true);
            trackerSocket.bind(trackerSocketAddress);
            topology.add(Filter.EVERYTHING, sourceServer);
            this.sourceServer = sourceServer;
            propertySupport = new PropertyChangeSupport(this);
            serverCheckerTask = new ServerCheckerTask();
            return;
        }
        catch (final IOException e) {
            try {
                trackerSocket.close();
            }
            catch (final IOException ignored) {
            }
            throw new IOException("Couldn't bind tracker socket to "
                    + trackerSocketAddress + ": " + e.toString());
        }
    }

    /**
     * Returns the local address of the this instance's (i.e., the tracker's)
     * server socket.
     * 
     * @return The local address of the this instance's server socket.
     */
    InetSocketAddress getServerAddress() {
        return new InetSocketAddress(trackerSocket.getInetAddress(),
                trackerSocket.getLocalPort());
    }

    /**
     * Returns the Internet socket address for reporting unavailable servers.
     * 
     * @return The Internet socket address for reporting unavailable servers.
     */
    InetSocketAddress getReportingAddress() {
        return serverCheckerTask.getInetSocketAddress();
    }

    /**
     * Executes this instance. Completes normally if and only if the current
     * thread is interrupted.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted
     * @throws IOException
     *             if a serious I/O error occurs
     */
    public Void call() throws InterruptedException, IOException {
        logger.trace("Starting up: {}", this);
        final String origThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(toString());
        try {
            final CompletionService<Void> completionService = new ExecutorCompletionService<Void>(
                    executor);
            final Future<Void> checkerFuture = completionService
                    .submit(serverCheckerTask);
            try {
                final Accepter accepterTask = new Accepter();
                completionService.submit(accepterTask);

                isRunningLatch.countDown();

                final Future<Void> future = completionService.take();
                if (!future.isCancelled()) {
                    try {
                        future.get();
                    }
                    catch (final ExecutionException e) {
                        final Throwable cause = e.getCause();
                        final Object task = future == checkerFuture
                                ? serverCheckerTask
                                : accepterTask;
                        if (cause instanceof IOException) {
                            throw new IOException("I/O error: " + task, cause);
                        }
                        throw new RuntimeException("Unexpected error: " + task,
                                cause);
                    }
                }
            }
            finally {
                executor.shutdownNow();
                awaitCompletion();
                try {
                    trackerSocket.close();
                }
                catch (final IOException e) {
                    if (!trackerSocket.isClosed()) {
                        logger.error("Couldn't close tracker's server-socket",
                                e);
                    }
                }
            }
        }
        finally {
            Thread.currentThread().setName(origThreadName);
            logger.trace("Done: {}", this);
        }
        return null;
    }

    /**
     * Waits until this instance is running.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    void waitUntilRunning() throws InterruptedException {
        isRunningLatch.await();
    }

    /**
     * Returns when this instance has released all its resources.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    void awaitCompletion() throws InterruptedException {
        Thread.interrupted();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }

    /**
     * Registers a server capable of satisfying a given file-selection filter.
     * 
     * @param server
     *            Address of the sink-node's server.
     * @param filter
     *            The file-selection filter.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code serverInfo == null || predicate == null}.
     */
    void register(final InetSocketAddress server, final Filter filter)
            throws IOException {
        topology.add(filter, server);
        propertySupport.firePropertyChange(NETWORK_TOPOLOGY_PROPERTY_NAME,
                null, new Topology(topology));
    }

    /**
     * Adds a property-change listener for the network topology.
     * 
     * @param listener
     *            The listener for network topology change events to be added.
     */
    void addNetworkTopologyChangeListener(final PropertyChangeListener listener) {
        propertySupport.addPropertyChangeListener(
                NETWORK_TOPOLOGY_PROPERTY_NAME, listener);
    }

    /**
     * Removes a property-change listener for the network topology.
     * 
     * @param listener
     *            The listener for network topology change events to be removed.
     */
    void removeNetworkTopologyChangeListener(
            final PropertyChangeListener listener) {
        propertySupport.removePropertyChangeListener(
                NETWORK_TOPOLOGY_PROPERTY_NAME, listener);
    }

    /**
     * Returns the current state of the network. The returned object is not a
     * copy.
     * 
     * @return The current state of the network.
     */
    Topology getNetwork() {
        return topology;
    }

    /**
     * Returns the current, filter-specific, state of the network. The returned
     * object isn't backed-up by this instance. Each server in the returned
     * instance will be able to satisfy, at least, the given filter.
     * 
     * @param filter
     *            The filter by which to subset the network topology.
     * 
     * @return The current state of the network.
     */
    Topology getNetwork(final Filter filter) {
        return topology.subset(filter);
    }

    /**
     * Returns the socket address of the tracker.
     * 
     * @return the socket address of the tracker.
     */
    SocketAddress getSocketAddress() {
        return trackerSocket.getLocalSocketAddress();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Tracker [trackerSocket=" + trackerSocket + ", sourceServer="
                + sourceServer + "]";
    }
}
