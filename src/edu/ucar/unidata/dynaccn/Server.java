package edu.ucar.unidata.dynaccn;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import net.jcip.annotations.GuardedBy;

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
     * The logging service.
     */
    private static final Logger                       logger           = LoggerFactory
                                                                               .getLogger(Server.class);
    /**
     * Executes port listeners.
     */
    private static final ExecutorService              listenerExecutor = Executors
                                                                               .newCachedThreadPool();
    /**
     * Executes peers created by this instance.
     */
    private static final ExecutorService              peerExecutor     = Executors
                                                                               .newCachedThreadPool();
    /**
     * The port listeners.
     */
    private final Listener[]                          listeners        = new Listener[Connection.SOCKET_COUNT];
    /**
     * The port listener manager.
     */
    private final TaskManager<Void>                   listenerManager  = new TaskManager<Void>(
                                                                               listenerExecutor);
    /**
     * The peers created by this instance.
     */
    @GuardedBy("itself")
    private final List<Peer>                          peers            = new LinkedList<Peer>();
    /**
     * The peer futures.
     */
    private final List<Future<?>>                     peerFutures      = Collections
                                                                               .synchronizedList(new LinkedList<Future<?>>());
    /**
     * The set of client-specific connections.
     */
    private final ConcurrentMap<ClientId, Connection> connections      = new ConcurrentHashMap<ClientId, Connection>();
    /**
     * The data clearing-house to use.
     */
    private final ClearingHouse                       clearingHouse;

    /**
     * Constructs from the clearing-house. Immediately starts listening for
     * connection attempts but doesn't process the attempts until method
     * {@link #call()} is called.
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
        if (null == clearingHouse) {
            throw new NullPointerException();
        }
        this.clearingHouse = clearingHouse;
        for (int i = 0; i < listeners.length; i++) {
            try {
                listeners[i] = new Listener();
            }
            catch (final IOException e) {
                while (--i >= 0) {
                    try {
                        listeners[i].close();
                    }
                    catch (final IOException e2) {
                        // ignored
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
     * Returns the ports on which this instance is listening.
     * 
     * @return The ports on which this instance is listening.
     */
    private int[] getPorts() {
        final int[] ports = new int[listeners.length];
        for (int i = 0; i < ports.length; i++) {
            ports[i] = listeners[i].getPort();
        }
        return ports;
    }

    /**
     * Executes this instance and completes normally if and only if the current
     * thread is interrupted.
     */
    @Override
    public Void call() {
        try {
            for (final Listener listener : listeners) {
                listenerManager.submit(listener);
            }
            try {
                listenerManager.waitUpon();
            }
            catch (final InterruptedException ignored) {
                // Implements thread interruption policy
            }
        }
        finally {
            listenerManager.cancel();
            for (final Future<?> future : peerFutures) {
                future.cancel(true);
            }
        }
        return null;
    }

    /**
     * Handles the creation of new local data by notifying all peers of the new
     * data.
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

    /**
     * Listens to a port for incoming connections.
     * 
     * Instances are thread-compatible but not thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private class Listener implements Callable<Void> {
        /**
         * The server socket.
         */
        private final ServerSocket listenerSocket;

        /**
         * Constructs from nothing. Immediately creates a server-side socket but
         * doesn't accept a connection until method {@link #call()} is called.
         * 
         * @throws IOException
         *             if the server-side socket couldn't be created.
         */
        Listener() throws IOException {
            listenerSocket = new ServerSocket();
            try {
                listenerSocket.setReuseAddress(true);
                listenerSocket.bind(new InetSocketAddress(0));
            }
            catch (final SocketException e) {
                try {
                    listenerSocket.close();
                }
                catch (final Exception e2) {
                    // ignored
                }
                throw e;
            }
            catch (final IOException e) {
                try {
                    listenerSocket.close();
                }
                catch (final Exception e2) {
                    // ignored
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
         * Doesn't return normally. Closes the listener socket.
         * 
         * @throws IOException
         *             if an I/O error occurs.
         */
        @Override
        public Void call() throws IOException {
            try {
                for (;;) {
                    final Socket socket = listenerSocket.accept();
                    try {
                        /*
                         * Get the port numbers of the client.
                         */
                        final DataInputStream dis = new DataInputStream(socket
                                .getInputStream());
                        final int[] clientPorts = new int[Connection.SOCKET_COUNT];
                        for (int i = 0; i < clientPorts.length; i++) {
                            clientPorts[i] = dis.readInt();
                        }
                        /*
                         * Create a unique client identifier.
                         */
                        final ClientId clientId = new ClientId(socket
                                .getInetAddress(), clientPorts);
                        /*
                         * Get the connection with this client.
                         */
                        Connection connection = new ServerConnection();
                        final Connection prevConnection = connections
                                .putIfAbsent(clientId, connection);
                        if (null != prevConnection) {
                            connection = prevConnection;
                        }
                        /*
                         * Add this socket to the connection and start the peer
                         * if ready.
                         */
                        if (connection.add(socket)) {
                            logger.debug("Server: {}", connection);
                            final Peer peer = new Peer(clearingHouse,
                                    connection);
                            synchronized (peers) {
                                peers.add(peer);
                            }
                            peerFutures.add(peerExecutor
                                    .submit(new FutureTask<Void>(peer) {
                                        @Override
                                        protected void done() {
                                            peerFutures.remove(this);
                                            synchronized (peers) {
                                                peers.remove(peer);
                                            }
                                        }
                                    }));
                        }
                    }
                    catch (final IOException e) {
                        socket.close();
                        throw e;
                    }
                }
            }
            finally {
                try {
                    listenerSocket.close();
                }
                catch (final IOException e) {
                    // ignored
                }
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
    }

    /**
     * Identifies a client.
     * 
     * Instances are immutable.
     * 
     * @author Steven R. Emmerson
     */
    private static class ClientId {
        /**
         * The Internet address of the host that's executing the client.
         */
        private final InetAddress inetAddress;
        /**
         * The ports that the client is using.
         */
        private final int[]       ports;

        /**
         * Constructs from the Internet address of the host that's executing the
         * client and the ports that the client is using.
         * 
         * @param inetAddress
         *            The Internet address of the host running the client.
         * @param ports
         *            The ports that the client is using.
         * @throws NullPointerException
         *             if {@code inetAddress == null || ports == null}.
         * @throws IllegalArgumentException
         *             if {@code ports.length != Connection.SOCKET_COUNT}.
         */
        ClientId(final InetAddress inetAddress, final int[] ports) {
            if (null == inetAddress) {
                throw new NullPointerException();
            }
            if (Connection.SOCKET_COUNT != ports.length) {
                throw new IllegalArgumentException();
            }
            this.inetAddress = inetAddress;
            this.ports = ports;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((inetAddress == null)
                    ? 0
                    : inetAddress.hashCode());
            result = prime * result + Arrays.hashCode(ports);
            return result;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final ClientId other = (ClientId) obj;
            if (inetAddress == null) {
                if (other.inetAddress != null) {
                    return false;
                }
            }
            else if (!inetAddress.equals(other.inetAddress)) {
                return false;
            }
            if (!Arrays.equals(ports, other.ports)) {
                return false;
            }
            return true;
        }
    }
}