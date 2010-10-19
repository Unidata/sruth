package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.NotThreadSafe;
import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles connections by clients.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class Server implements Callable<Void> {
    /**
     * A listener for client-disconnect events.
     * 
     * Instances are thread-compatible but not thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @NotThreadSafe
    static abstract class DisconnectListener {
        /**
         * Handles client-disconnect events.
         * 
         * @param serverInfo
         *            Information on the server associated with the disconnected
         *            client.
         */
        abstract void clientDisconnected(ServerInfo serverInfo);
    }

    /**
     * Support for listeners of client-disconnect events.
     * 
     * Instances are thread-safe.
     */
    @ThreadSafe
    private class DisconnectListenerSupport {
        @GuardedBy("this")
        private final LinkedList<DisconnectListener> list = new LinkedList<DisconnectListener>();

        /**
         * Adds a listener. The listener will be notified of client-disconnect
         * events as many times as it is added.
         * 
         * @param listener
         *            The listener of client-disconnect events.
         * @return
         * @throws NullPointerException
         *             if {@code listener == null}.
         */
        synchronized void add(final DisconnectListener listener) {
            if (listener == null) {
                throw new NullPointerException();
            }
            list.add(listener);
        }

        /**
         * Notifies all listeners of a client disconnect.
         * 
         * @param serverInfo
         *            Information on the disconnected client.
         * @throws NullPointerException
         *             if {@code serverInfo == null}.
         */
        synchronized void notify(final ServerInfo serverInfo) {
            for (final DisconnectListener listener : list) {
                listener.clientDisconnected(serverInfo);
            }
        }
    }

    /**
     * Listens to a port for incoming connections.
     * 
     * Instances are thread-compatible but not thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private class Listener extends BlockingTask<Void> {
        /**
         * The server socket.
         */
        private final ServerSocket listenerSocket;

        /**
         * Constructs from a range of port numbers. If {@code minPort == 0 &&
         * maxPort == 0} then the operating-system will assign an ephemeral
         * port.
         * 
         * @param portSet
         *            The set of candidate port numbers.
         * @throws IOException
         *             if an unused port in the given range couldn't be found.
         * @throws NullPointerException
         *             if {@code portSet == null}.
         * @throws SocketException
         *             if a server-side socket couldn't be created.
         */
        Listener(final PortNumberSet portSet) throws IOException,
                SocketException {
            listenerSocket = new ServerSocket();
            try {
                listenerSocket.setReuseAddress(true);
                for (final int port : portSet) {
                    try {
                        listenerSocket.bind(new InetSocketAddress(port));
                        return;
                    }
                    catch (final IOException ignored) {
                    }
                }
                throw new IOException("Couldn't find unused port in " + portSet);
            }
            catch (final SocketException e) {
                try {
                    listenerSocket.close();
                }
                catch (final IOException ignored) {
                }
                throw e;
            }
        }

        /**
         * Returns the port on which this instance is listening.
         */
        int getPort() {
            return listenerSocket.getLocalPort();
        }

        /**
         * Handles a connection attempt from a client. If the connection attempt
         * fails due to a non-severe I/O error (e.g., the client's socket
         * numbers couldn't be read), then the attempt is logged but no
         * exception is thrown.
         * 
         * @throws IOException
         *             if a severe I/O error occurs.
         * @throws RejectedExecutionException
         *             if subtasks can't be submitted.
         */
        private final void handleConnectionAttempt() throws IOException {
            final Socket socket = listenerSocket.accept();
            try {
                /*
                 * Get the port numbers of the client.
                 */
                final int[] clientPorts = ConnectionToClient
                        .getRemoteServerPorts(socket);
                /*
                 * Create a unique client identifier.
                 */
                final ServerInfo serverInfo = new ServerInfo(socket
                        .getInetAddress(), clientPorts);
                /*
                 * Get the connection with this client.
                 */
                ConnectionToClient connection = new ConnectionToClient();
                final ConnectionToClient prevConn = connections.putIfAbsent(
                        serverInfo, connection);
                if (null != prevConn) {
                    connection = prevConn;
                }
                /*
                 * Add this socket to the connection and start the peer if
                 * ready.
                 */
                if (connection.add(socket)) {
                    connections.remove(serverInfo);
                    final Peer peer = new Peer(clearingHouse, connection);
                    synchronized (peers) {
                        peers.add(peer);
                    }
                    logger.debug("Peer starting: {}", connection);
                    peerExecutor.submit(new FutureTask<Void>(peer) {
                        @Override
                        protected void done() {
                            synchronized (peers) {
                                peers.remove(peer);
                            }
                            disconnectListenerSupport.notify(serverInfo);
                            if (!isCanceled()) {
                                try {
                                    get();
                                    logger.debug("Peer completed: {}", peer);
                                }
                                catch (final ExecutionException e) {
                                    final Throwable cause = e.getCause();
                                    if (cause instanceof InterruptedException) {
                                        // ignored
                                    }
                                    else if (cause instanceof ConnectException) {
                                        logger.info(cause.toString());
                                    }
                                    else if (cause instanceof IOException) {
                                        logger.error("Peer failure: " + peer,
                                                cause);
                                    }
                                    else {
                                        throw Util.launderThrowable(cause);
                                    }
                                }
                                catch (final InterruptedException ignored) {
                                    logger.debug("Interrupted");
                                }
                            }
                        }
                    });
                }
            }
            catch (final IOException e) {
                logger.info("Couldn't get remote port numbers from {}: {}",
                        socket, e.toString());
                socket.close();
            }
        }

        /**
         * Doesn't return normally. Closes the listener socket.
         * 
         * @throws IOException
         *             if a severe I/O error occurs.
         */
        @Override
        public Void call() throws IOException {
            final String origThreadName = Thread.currentThread().getName();
            Thread.currentThread().setName(toString());
            try {
                for (;;) {
                    handleConnectionAttempt();
                }
            }
            finally {
                try {
                    listenerSocket.close();
                }
                catch (final IOException e) {
                    // ignored
                }
                Thread.currentThread().setName(origThreadName);
            }
        }

        /**
         * Closes this instance.
         * 
         * @throws IOException
         *             if an error occurs while closing the socket.
         */
        void close() throws IOException {
            listenerSocket.close();
        }

        @Override
        protected void stop() {
            try {
                close();
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
            return getClass().getSimpleName() + " [listenerSocket="
                    + listenerSocket + "]";
        }
    }

    /**
     * The logging service.
     */
    private static final Logger                                 logger                    = LoggerFactory
                                                                                                  .getLogger(Server.class);
    /**
     * Executes peers created by this instance.
     */
    private final ExecutorService                               peerExecutor              = Executors
                                                                                                  .newCachedThreadPool();
    /**
     * Executes port listeners.
     */
    private final ExecutorService                               listenerExecutor          = new CancellingExecutor(
                                                                                                  Connection.SOCKET_COUNT,
                                                                                                  Connection.SOCKET_COUNT,
                                                                                                  0,
                                                                                                  TimeUnit.SECONDS,
                                                                                                  new SynchronousQueue<Runnable>());
    /**
     * The port-listener task-manager.
     */
    private final ExecutorCompletionService<Void>               listenerManager           = new ExecutorCompletionService<Void>(
                                                                                                  listenerExecutor);
    /**
     * The port listeners.
     */
    private final Listener[]                                    listeners                 = new Listener[Connection.SOCKET_COUNT];
    /**
     * The peers created by this instance.
     */
    @GuardedBy("itself")
    private final List<Peer>                                    peers                     = new LinkedList<Peer>();
    /**
     * The set of client-specific connections.
     */
    private final ConcurrentMap<ServerInfo, ConnectionToClient> connections               = new ConcurrentHashMap<ServerInfo, ConnectionToClient>();
    /**
     * The data clearing-house to use.
     */
    private final ClearingHouse                                 clearingHouse;
    /**
     * Support for listeners of client-disconnect events.
     */
    private final DisconnectListenerSupport                     disconnectListenerSupport = new DisconnectListenerSupport();

    /**
     * Constructs from the clearing-house. Immediately starts listening for
     * connection attempts but doesn't process the attempts until method
     * {@link #call()} is called. The ports used by the server will be ephemeral
     * one assigned by the operating-system.
     * 
     * By default, the resulting instance will listen on all available
     * interfaces.
     * 
     * @param clearingHouse
     *            The clearing-house to use.
     * @throws IOException
     *             if a port can't be listened to.
     * @throws NullPointerException
     *             if {@code clearingHouse == null}.
     */
    Server(final ClearingHouse clearingHouse) throws IOException {
        this(clearingHouse, (int[]) null);
    }

    /**
     * Constructs from the clearing-house and port numbers for the server.
     * Immediately starts listening for connection attempts but doesn't process
     * the attempts until method {@link #call()} is called.
     * 
     * By default, the resulting instance will listen on all available
     * interfaces.
     * 
     * @param clearingHouse
     *            The clearing-house to use.
     * @param serverPorts
     *            Port numbers for the server or {@code null}. A port number of
     *            zero will cause the operating-system to assign an ephemeral
     *            port. If {@code null}, then all ports will be ephemeral.
     * @throws IOException
     *             if an unused port in the given range couldn't be found.
     * @throws NullPointerException
     *             if {@code clearingHouse == null}.
     * @throws SocketException
     *             if a server-side socket couldn't be created.
     */
    Server(final ClearingHouse clearingHouse, final int[] serverPorts)
            throws IOException {
        if (null == clearingHouse) {
            throw new NullPointerException();
        }
        this.clearingHouse = clearingHouse;
        for (int i = 0; i < listeners.length; i++) {
            try {
                final int minPort = (serverPorts == null)
                        ? 0
                        : serverPorts[i];
                final PortNumberSet portSet = PortNumberSet.getInstance(
                        minPort, minPort);
                listeners[i] = new Listener(portSet);
            }
            catch (final IOException e) {
                while (--i >= 0) {
                    try {
                        listeners[i].close();
                    }
                    catch (final IOException ignored) {
                    }
                }
                throw e;
            }
        }
    }

    /**
     * Constructs from the clearing-house and an range of port numbers for the
     * server. Immediately starts listening for connection attempts but doesn't
     * process the attempts until method {@link #call()} is called.
     * 
     * If {@code minPort == 0 && maxPort == 0} then the operating-system will
     * assign ephemeral ports.
     * 
     * The resulting instance will listen on all available interfaces.
     * 
     * @param clearingHouse
     *            The clearing-house to use.
     * @param portSet
     *            The set of candidate port numbers.
     * @throws IOException
     *             if an unused port in the given range couldn't be found.
     * @throws NullPointerException
     *             if {@code clearingHouse == null || portSet == null}.
     * @throws SocketException
     *             if a server-side socket couldn't be created.
     */
    Server(final ClearingHouse clearingHouse, final PortNumberSet portSet)
            throws IOException, SocketException {
        if (null == clearingHouse || null == portSet) {
            throw new NullPointerException();
        }
        this.clearingHouse = clearingHouse;
        for (int i = 0; i < listeners.length; i++) {
            try {
                listeners[i] = new Listener(portSet);
            }
            catch (final IOException e) {
                while (--i >= 0) {
                    try {
                        listeners[i].close();
                    }
                    catch (final IOException ignored) {
                    }
                }
                throw e;
            }
        }
    }

    /**
     * Returns information on the server. This method may be called immediately
     * after construction of the instance.
     * 
     * @return Information on the server.
     * @throws UnknownHostException
     *             if the IP address of the local host can't be obtained.
     */
    ServerInfo getServerInfo() throws UnknownHostException {
        return new ServerInfo(InetAddress.getLocalHost(), getPorts());
    }

    /**
     * Returns the ports on which this instance is listening in ascending
     * numerical order.
     * 
     * @return The ports on which this instance is listening in ascending
     *         numerical order.
     */
    private int[] getPorts() {
        final int[] ports = new int[listeners.length];
        for (int i = 0; i < ports.length; i++) {
            ports[i] = listeners[i].getPort();
        }
        Arrays.sort(ports);
        return ports;
    }

    /**
     * Returns the pathname of the root of the file-tree.
     * 
     * @return The pathname of the root of the file-tree.
     */
    Path getRootDir() {
        return clearingHouse.getRootDir();
    }

    /**
     * Adds a listener for client disconnections.
     * 
     * @param listener
     *            The listener for client disconnection events.
     * @throws NullPointerException
     *             if {@code listener == null}.
     */
    void addDisconnectListener(final DisconnectListener listener) {
        disconnectListenerSupport.add(listener);
    }

    /**
     * Executes this instance. Never returns.
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
        try {
            for (final Listener listener : listeners) {
                listenerManager.submit(listener);
            }
            final Future<Void> future = listenerManager.take();
            try {
                future.get();
            }
            catch (final ExecutionException e) {
                final Throwable cause = e.getCause();
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                }
                throw Util.launderThrowable(cause);
            }
            catch (final InterruptedException e) {
                logger.debug("Interrupted");
                throw e;
            }
        }
        finally {
            peerExecutor.shutdownNow();
            listenerExecutor.shutdownNow();
            Thread.currentThread().setName(origThreadName);
        }
        return null;
    }

    /**
     * Returns the number of clients that this instance is serving.
     * 
     * @return The number of clients that this instance is serving.
     */
    int getClientCount() {
        synchronized (peers) {
            return peers.size();
        }
    }

    /**
     * Handles the creation of new local data by notifying all appropriate
     * peers.
     * 
     * @param spec
     *            Specification of the new local data.
     */
    void newData(final FilePieceSpecSet spec) {
        synchronized (peers) {
            for (final Peer peer : peers) {
                peer.newData(spec);
            }
        }
    }

    /**
     * Responds to a file or file category being removed by notifying all remote
     * clients.
     * 
     * @param fileId
     *            Identifier of the file or category.
     */
    void removed(final FileId fileId) {
        synchronized (peers) {
            for (final Peer peer : peers) {
                peer.notifyRemoteOfRemovals(fileId);
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
        return "Server [clearingHouse=" + clearingHouse + ", ports="
                + Arrays.toString(getPorts()) + ", connections=("
                + connections.size() + ")]";
    }
}