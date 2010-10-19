/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

/**
 * The client-side of a connection to a server.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class ConnectionToServer extends Connection {
    /**
     * Constructs from information on the server.
     * 
     * @param serverInfo
     *            Information on the server.
     * @throws IllegalArgumentException
     *             if {@code serverPorts.length != SOCKET_COUNT}.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code serverInfo == null}.
     */
    ConnectionToServer(final ServerInfo serverInfo) throws IOException {
        final int[] serverPorts = serverInfo.getPorts();

        if (serverPorts.length != SOCKET_COUNT) {
            throw new IllegalArgumentException("serverPorts.length: "
                    + serverPorts.length);
        }
        try {
            final InetAddress serverAddress = serverInfo.getInetAddress();
            final Socket[] sockets = new Socket[SOCKET_COUNT];
            for (int i = 0; i < SOCKET_COUNT; i++) {
                sockets[i] = new Socket(serverAddress, serverPorts[i]);
                add(sockets[i]);
            }
            /*
             * For each socket:
             */
            for (final Socket socket : sockets) {
                /*
                 * Write this client's port numbers on the socket to help
                 * identify this client.
                 */
                final DataOutputStream stream = new DataOutputStream(socket
                        .getOutputStream());
                for (final Socket sock : sockets) {
                    stream.writeInt(sock.getLocalPort());
                }
                stream.flush();
            }
        }
        catch (final IOException e) {
            close();
            throw e;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.ucar.unidata.dynaccn.Connection#getServerPort(java.net.Socket)
     */
    @Override
    protected int getServerPort(final Socket socket) {
        return socket.getPort();
    }
}
