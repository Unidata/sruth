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
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.prefs.Preferences;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;

import edu.ucar.unidata.sruth.Connection.Stream;

/**
 * Handles connections by clients.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
abstract class Server extends UninterruptibleTask<Void> {
    /**
     * Manages a collection of servlets. A "servlet" is a client's individual
     * server.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    final static class ServletManager {
        /**
         * The server-side counterpart of a client: there is a one-to-to
         * correspondence between clients and servlets.
         * <p>
         * Instances are thread-safe.
         * 
         * @author Steven R. Emmerson
         */
        @ThreadSafe
        private class Servlet extends UninterruptibleTask<Void> {
            /**
             * The connection to the client.
             */
            private final Connection              connection;
            /**
             * The client's data-filter.
             */
            private final AtomicReference<Filter> clientFilterRef = new AtomicReference<Filter>();
            /**
             * The local peer that communicates with the remote peer.
             */
            private final AtomicReference<Peer>   peerRef         = new AtomicReference<Peer>();

            /**
             * Constructs from an initial connection by the client.
             * 
             * @param connection
             *            The connection to the client.
             * @throws NullPointerException
             *             if {@code connection == null}.
             * @throws NullPointerException
             *             if {@code clearingHouse == null}.
             */
            Servlet(final Connection connection) {
                if (connection == null) {
                    throw new NullPointerException();
                }

                this.connection = connection;
            }

            @Override
            public Void call() throws InterruptedException, IOException {
                logger.debug("Starting up: {}", this);
                final Predicate predicate = clearingHouse.getPredicate();
                try {
                    // NB: Client sends filter first, then server
                    final Stream requestStream = connection.getRequestStream();
                    final Filter filter = (Filter) requestStream
                            .receiveObject(Connection.SO_TIMEOUT);
                    clientFilterRef.set(filter);
                    try {
                        final Filter serverFilter = predicate
                                .getIncludingFilter(filter);
                        requestStream.send(serverFilter);

                        final Peer peer = new Peer(clearingHouse, connection,
                                serverFilter, filter);
                        peerRef.set(peer);
                        if (addIfAppropriate()) {
                            execute();
                        }
                    }
                    catch (final IOException e) {
                        if (!isCancelled()) {
                            logger.error(
                                    "Couldn't send server's filter on {}: {}",
                                    connection, e.toString());
                        }
                    }
                }
                catch (final InterruptedException e) {
                    logger.debug("Interrupt: {}", this);
                    throw e;
                }
                catch (final Exception e) {
                    if (!isCancelled()) {
                        // Might be just a server-availability test
                        logger.trace(
                                "Couldn't receive client's filter on {}: {}",
                                connection, e.toString());
                    }
                }
                finally {
                    connection.close();
                    logger.debug("Done: {}", this);
                }
                return null;
            }

            @Override
            protected void stop() {
                connection.close();
            }

            /**
             * Adds this instance to the servlet manager if appropriate. This
             * method might remove another instance from the servlet manager.
             * 
             * @return {@code true} if and only if this instance was added to
             *         the servlet manager.
             */
            private boolean addIfAppropriate() {
                boolean addInstance = false;
                synchronized (ServletManager.this) {
                    if (servlets.size() < maxNumActiveServlets) {
                        addInstance = true;
                    }
                    else {
                        for (final Servlet that : servlets) {
                            if (this.isBetterThan(that)) {
                                that.cancel();
                                addInstance = true;
                                break;
                            }
                        }
                    }
                    if (addInstance) {
                        servlets.add(this);
                        numOutstandingServlets--;
                    }
                    else {
                        logger.debug("Not sufficiently better: {}", this);
                    }
                }
                return addInstance;
            }

            /**
             * Indicates if this instance is "better" than another instance.
             * Such instances are favored for servicing.
             * 
             * @param that
             *            The other instance.
             * @return {@code true} if this instance is better than the other
             *         instance.
             * @throws NullPointerException
             *             if {@code that == null}.
             */
            boolean isBetterThan(final Servlet that) {
                final Filter thisFilter = this.clientFilterRef.get();
                final Filter thatFilter = that.clientFilterRef.get();
                return thisFilter.includes(thatFilter)
                        && !thisFilter.equals(thatFilter);
            }

            /**
             * Executes this instance.
             * 
             * @throws InterruptedException
             *             if the current thread is interrupted.
             */
            private void execute() throws InterruptedException {
                final Peer peer = peerRef.get();
                try {
                    peer.call();
                }
                catch (final Exception e) {
                    if (!isCancelled()) {
                        if (e instanceof InterruptedException) {
                        }
                        else if (e instanceof EOFException) {
                            logger.info(
                                    "Connection closed by remote client: {}: {}",
                                    connection, e);
                        }
                        else if (e instanceof ConnectException) {
                            logger.info(
                                    "Couldn't connect to remote client: {}: {}",
                                    connection, e);
                        }
                        else if (e instanceof SocketException) {
                            logger.info(
                                    "Connection to remote client closed: {}: {}",
                                    connection, e);
                        }
                        else if (e instanceof IOException) {
                            logger.error("Servlet I/O failure: " + this, e);
                        }
                        else {
                            logger.error("Logic error", e);
                        }
                    }
                }
                finally {
                    synchronized (ServletManager.this) {
                        servlets.remove(this);
                    }
                }
            }

            /**
             * Handles the creation of new local data by informing the peer.
             * 
             * @param spec
             *            Specification of the new local data.
             */
            void newData(final FilePieceSpecSet spec) {
                final Peer peer = peerRef.get();
                if (peer != null) {
                    peer.newData(spec);
                }
            }

            /**
             * Responds to a file being removed by notifying the peer.
             * 
             * @param fileId
             *            Identifier of the file.
             */
            void removed(final FileId fileId) {
                final Peer peer = peerRef.get();
                if (peer != null) {
                    peer.notifyRemoteOfRemovals(fileId);
                }
            }

            /*
             * (non-Javadoc)
             * 
             * @see java.lang.Object#toString()
             */
            @Override
            public String toString() {
                return "Servlet [connection=" + connection + ", clientFilter="
                        + clientFilterRef.get() + "]";
            }
        }

        /**
         * The local server-socket.
         */
        private final ServerSocket       serverSocket;
        /**
         * The data-exchange clearing house.
         */
        private final ClearingHouse      clearingHouse;
        /**
         * Executes servlets created by this instance.
         */
        private final CancellingExecutor servletExecutor;
        /**
         * The map from servlet-futures to the servlets created and managed by
         * this instance.
         */
        @GuardedBy("this")
        private final List<Servlet>      servlets = new LinkedList<Servlet>();
        /**
         * The maximum number of active servlets.
         */
        private final int                maxNumActiveServlets;
        /**
         * The maximum number of outstanding servlets.
         */
        private final int                maxNumOutstandingServlets;
        /**
         * The number of outstanding servlets.
         */
        @GuardedBy("this")
        private int                      numOutstandingServlets;

        /**
         * Constructs from information on the local node, the data-exchange
         * clearing house, and limits on the number of active and outstanding
         * servlets.
         * 
         * @param serverSocket
         *            The local server-socket.
         * @param clearingHouse
         *            The data-exchange clearing house.
         * @param maxNumOutstandingServlets
         *            Maximum number of outstanding servlets. A servlet is
         *            outstanding upon construction. An outstanding servlet
         *            either terminates or becomes an active servlet.
         * @param maxNumActiveServlets
         *            Maximum number of active servlets. An active servlet
         *            exchanges data with its client.
         * @throws NullPointerException
         *             if {@code serverSocket == null}.
         * @throws NullPointerException
         *             if {@code clearingHouse == null}.
         */
        ServletManager(final ServerSocket serverSocket,
                final ClearingHouse clearingHouse,
                final int maxNumOutstandingServlets,
                final int maxNumActiveServlets) {
            if (serverSocket == null) {
                throw new NullPointerException();
            }
            if (clearingHouse == null) {
                throw new NullPointerException();
            }
            if (maxNumOutstandingServlets <= 0) {
                throw new IllegalArgumentException();
            }
            if (maxNumActiveServlets <= 0) {
                throw new IllegalArgumentException();
            }
            this.serverSocket = serverSocket;
            this.clearingHouse = clearingHouse;
            this.maxNumActiveServlets = maxNumActiveServlets;
            this.maxNumOutstandingServlets = maxNumOutstandingServlets;
            servletExecutor = new CancellingExecutor(0, maxNumActiveServlets
                    + maxNumOutstandingServlets, 60, TimeUnit.SECONDS,
                    new SynchronousQueue<Runnable>());
        }

        /**
         * If appropriate, accepts a new connection to a client. The connection
         * might not be accepted. If it is accepted, then a servlet is executed
         * in a separate thread to service the client.
         * 
         * @param connection
         *            The new connection to a client.
         * @throws NullPointerException
         *             if {@code connection == null}.
         * @throws RejectedExecutionException
         *             if {@link #shutdownNow()} has been called.
         */
        synchronized void submit(final Connection connection) {
            if (numOutstandingServlets >= maxNumOutstandingServlets) {
                logger.debug("Too many outstanding connections: {}", connection);
                connection.close();
            }
            else {
                final Servlet servlet = new Servlet(connection);
                try {
                    servletExecutor.submit(servlet);
                    numOutstandingServlets++;
                }
                catch (final RejectedExecutionException e) {
                    throw e;
                }
            }
        }

        /**
         * Shuts this instance down. Cancels all executing servlets and stops
         * acceptance of new connections.
         */
        void shutdownNow() {
            logger.trace("Shutting down {}", this);
            servletExecutor.shutdownNow();
        }

        /**
         * Handles the creation of new local data by notifying all servlets.
         * 
         * @param spec
         *            Specification of the new local data.
         */
        synchronized void newData(final FilePieceSpecSet spec) {
            for (final Servlet servlet : servlets) {
                servlet.newData(spec);
            }
        }

        /**
         * Responds to a file being removed by notifying all servlets.
         * 
         * @param fileId
         *            Identifier of the file.
         */
        synchronized void removed(final FileId fileId) {
            for (final Servlet servlet : servlets) {
                servlet.removed(fileId);
            }
        }

        /**
         * Returns the number of active servlets that this instance is managing.
         * 
         * @return The number of active servlets that this instance is managing.
         */
        synchronized int size() {
            return servlets.size();
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public synchronized String toString() {
            return "ServletManager [serverSocket=" + serverSocket
                    + ", servlets=(" + size() + ")]";
        }
    }

    /**
     * The logging service.
     */
    private static final Logger     logger                               = Util.getLogger();

    /**
     * The maximum number of active servlets.
     */
    private static final int        MAX_NUM_ACTIVE_SERVLETS;
    private static final String     MAX_NUM_ACTIVE_SERVLETS_KEY          = "maximum number of active servlets";
    private static final int        MAX_NUM_ACTIVE_SERVLETS_DEFAULT      = 8;
    /**
     * The maximum number of outstanding servlets.
     */
    private static final int        MAX_NUM_OUTSTANDING_SERVLETS;
    private static final String     MAX_NUM_OUTSTANDING_SERVLETS_KEY     = "maximum number of outstanding servlets";
    private static final int        MAX_NUM_OUTSTANDING_SERVLETS_DEFAULT = 4;

    static {
        final Preferences prefs = Preferences.userNodeForPackage(Server.class);

        MAX_NUM_ACTIVE_SERVLETS = prefs.getInt(MAX_NUM_ACTIVE_SERVLETS_KEY,
                MAX_NUM_ACTIVE_SERVLETS_DEFAULT);
        if (MAX_NUM_ACTIVE_SERVLETS < 0) {
            throw new IllegalArgumentException("Invalid preference: \""
                    + MAX_NUM_ACTIVE_SERVLETS_KEY + "\"="
                    + MAX_NUM_ACTIVE_SERVLETS);
        }

        MAX_NUM_OUTSTANDING_SERVLETS = prefs.getInt(
                MAX_NUM_OUTSTANDING_SERVLETS_KEY,
                MAX_NUM_OUTSTANDING_SERVLETS_DEFAULT);
        if (MAX_NUM_OUTSTANDING_SERVLETS < 0) {
            throw new IllegalArgumentException("Invalid preference: \""
                    + MAX_NUM_OUTSTANDING_SERVLETS_KEY + "\"="
                    + MAX_NUM_OUTSTANDING_SERVLETS);
        }
    }

    /**
     * The factory for creating connections to clients.
     */
    private final ConnectionFactory connectionFactory;
    /**
     * The manager of servlets.
     */
    private final ServletManager    servletManager;
    /**
     * The socket used by this instance.
     */
    private final ServerSocket      serverSocket;
    /**
     * The "isRunning" latch.
     */
    private final CountDownLatch    isRunningLatch                       = new CountDownLatch(
                                                                                 1);

    private final AtomicLong        startTimeRef                         = new AtomicLong(
                                                                                 System.currentTimeMillis());

    /**
     * Constructs from the data-exchange clearing-house. Immediately starts
     * listening for connection attempts but doesn't process the attempts until
     * method {@link #call()} is called. This constructor is equivalent to
     * {@link #Server(ClearingHouse, PortNumberSet) Server(clearingHouse, new
     * InetSocketAddressSet())},
     * 
     * @param clearingHouse
     *            The data-exchange clearing-house.
     * @throws IOException
     *             if the server-socket can't be bound to a port.
     * @throws NullPointerException
     *             if {@code clearingHouse == null}.
     * @see #Server(ClearingHouse, InetSocketAddressSet)
     */
    Server(final ClearingHouse clearingHouse) throws IOException {
        this(clearingHouse, new InetSocketAddressSet());
    }

    /**
     * Constructs from the clearing-house for data-exchange and a set of
     * candidate Internet socket address. Immediately starts listening for
     * connection attempts but doesn't process the attempts until method
     * {@link #call()} is called. The upper limit on the number of servlets is
     * specified by the user-preference {@value #MAX_NUM_ACTIVE_SERVLETS_KEY}
     * (default {@value #MAX_NUM_ACTIVE_SERVLETS_DEFAULT}).
     * 
     * @param clearingHouse
     *            The data-exchange clearing-house.
     * @param inetSockAddrSet
     *            The set of candidate Internet socket addresses.
     * @throws IOException
     *             if an unused port in the given range couldn't be found.
     * @throws NullPointerException
     *             if {@code clearingHouse == null}.
     * @throws NullPointerException
     *             if {@code inetSockAddrSet == null}.
     * @throws SocketException
     *             if a server-side socket couldn't be created.
     */
    Server(final ClearingHouse clearingHouse,
            final InetSocketAddressSet inetSockAddrSet) throws IOException,
            SocketException {
        // TODO: Set limit on number of outstanding connections
        serverSocket = new ServerSocket();
        try {
            adjustSocket(serverSocket);
            if (!inetSockAddrSet.bind(serverSocket)) {
                throw new IOException("Couldn't find unused port in "
                        + inetSockAddrSet);
            }
            servletManager = new ServletManager(serverSocket, clearingHouse,
                    MAX_NUM_OUTSTANDING_SERVLETS, MAX_NUM_ACTIVE_SERVLETS);
            connectionFactory = new ConnectionFactory(
                    (InetSocketAddress) serverSocket.getLocalSocketAddress());
        }
        catch (final SocketException e) {
            try {
                serverSocket.close();
            }
            catch (final IOException ignored) {
            }
            throw e;
        }
    }

    /**
     * Adjusts the socket that the server will use.
     * 
     * @throws IOException
     *             if an I/O error occurs.
     */
    abstract void adjustSocket(ServerSocket socket) throws IOException;

    /**
     * Returns the address of the server. May be called immediately after
     * construction.
     * 
     * @return The address of the server.
     */
    InetSocketAddress getSocketAddress() {
        return new InetSocketAddress(serverSocket.getInetAddress(),
                serverSocket.getLocalPort());
    }

    /**
     * Returns the address of the server's socket.
     * 
     * @return the address of the server's socket.
     */
    InetSocketAddress getInetSocketAddress() {
        return (InetSocketAddress) serverSocket.getLocalSocketAddress();
    }

    /**
     * Executes this instance. Services connections by clients. Returns only if
     * Canceled. The server-socket is closed upon return.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if a severe I/O error occurs.
     */
    @Override
    public Void call() throws InterruptedException, IOException {
        final String origThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(toString());
        logger.info("Starting up: {}", this);
        isRunningLatch.countDown();
        startTimeRef.set(System.currentTimeMillis());
        logger.debug("Server listening on {}", serverSocket);

        try {
            for (;;) {
                // TODO: Implement graceful denial-of-service degradation
                final Socket socket = serverSocket.accept();
                try {
                    final Connection connection = connectionFactory
                            .getInstance(socket);
                    /*
                     * Submit the connection if it's ready.
                     */
                    if (connection != null) {
                        servletManager.submit(connection);
                    }
                }
                catch (final IOException e) {
                    logger.error("Error on {}: {}", socket, e.toString());
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
                serverSocket.close();
            }
            catch (final IOException ignored) {
            }
            servletManager.shutdownNow();
            logger.info("Done: {}", this);
            Thread.currentThread().setName(origThreadName);
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
     * Stops this instance: closes the server-socket and terminates any and all
     * executing servlets.
     */
    @Override
    protected void stop() {
        try {
            logger.debug("Closing {}", serverSocket);
            serverSocket.close();
        }
        catch (final Error e) {
            logger.error("Error closing server-socket", e);
        }
        catch (final IOException ignored) {
        }
        servletManager.shutdownNow();
    }

    /**
     * Returns the number of clients currently being serviced.
     * 
     * @return The number of clients being served.
     */
    int getServletCount() {
        return servletManager.size();
    }

    /**
     * Handles the creation of new local data by notifying the servlet manager.
     * 
     * @param spec
     *            Specification of the newly-created data.
     */
    void newData(final FilePieceSpecSet spec) {
        servletManager.newData(spec);
    }

    /**
     * Handles the removal of a file from the archive.
     * 
     * @param fileId
     *            Identifier of the removed file.
     */
    void removed(final FileId fileId) {
        servletManager.removed(fileId);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return getClass().getSimpleName() + " [server socket=" + serverSocket
                + ", servlets=(" + servletManager.size()
                + "), incomplete connections=("
                + connectionFactory.getNumIncomplete() + ")]";
    }
}