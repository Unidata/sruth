/**
 * Copyright 2011 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import net.jcip.annotations.ThreadSafe;

/**
 * Factory for creating {@link Connection}s.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class ConnectionFactory {
    /**
     * The set of incomplete, not-yet-ready connections.
     */
    final ConcurrentMap<Object, ConnectionToClient> connections = new ConcurrentHashMap<Object, ConnectionToClient>();

    /**
     * The address of the local server.
     */
    private final InetSocketAddress                 localServer;

    /**
     * Constructs from the address of the local server.
     * 
     * @param localServer
     *            The address of the local server.
     * @throws NullPointerException
     *             if {@code localServer == null}.
     */
    ConnectionFactory(final InetSocketAddress localServer) {
        // TODO: Create constructor with limit on number of incomplete
        // connections
        if (localServer == null) {
            throw new NullPointerException();
        }
        this.localServer = localServer;
    }

    /**
     * Returns a {@link ConnectionToServer}.
     * <p>
     * This method is potentially lengthy.
     * 
     * @param remoteServer
     *            The address of the remote server.
     * @throws IOException
     *             if a connection can't be made to the remove server.
     * @throws NullPointerException
     *             if {@code remoteServerAddress == null}.
     */
    ConnectionToServer getInstance(final InetSocketAddress remoteServer)
            throws IOException {
        return new ConnectionToServer(localServer, remoteServer);
    }

    /**
     * Returns the {@link ConnectionToClient} corresponding to a just-accepted
     * socket or {@code null} if the connection isn't yet ready. If {@code null}
     * is returned, then an incomplete connection exists that contains the input
     * socket. If a {@link Connection} is returned, then it is no longer a
     * member of the incomplete connections.
     * <p>
     * This method is potentially lengthy.
     * 
     * @param socket
     *            The socket that a server just accepted.
     * @return The corresponding {@link ConnectionToClient} or {@code null} if
     *         the connection is not yet ready.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code socket == null}.
     */
    Connection getInstance(final Socket socket) throws IOException {
        socket.setSoLinger(false, 0); // because flush() always called
        socket.setTcpNoDelay(false); // because flush() called when appropriate
        socket.setKeepAlive(true);

        ConnectionToClient connection = new ConnectionToClient(socket);

        final ConnectionToClient prevConnection = connections.putIfAbsent(
                connection.getConnectionId(), connection);

        if (prevConnection != null) {
            prevConnection.add(connection);
            connection = prevConnection;
        }

        if (!connection.isReady()) {
            connection = null;
        }
        else {
            connections.remove(connection.getConnectionId());
        }

        return connection;
    }

    /**
     * Returns the number of incomplete (i.e., not-yet-ready) connections.
     * 
     * @return The number of incomplete connections.
     */
    int getNumIncomplete() {
        return connections.size();
    }
}
