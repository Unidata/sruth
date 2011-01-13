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
 * The client-side of a connection to a remote server.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class ConnectionToServer extends Connection {
    /**
     * Constructs from information on the remote server and the port numbers
     * used by the local server.
     * 
     * @param remoteServerInfo
     *            Information on the remote server.
     * @param localServerPorts
     *            The port numbers used by the local server.
     * @throws IllegalArgumentException
     *             if {@code remoteServerInfo.getPorts().length != SOCKET_COUNT}
     *             .
     * @throws IllegalArgumentException
     *             if {@code localServerPorts.length != SOCKET_COUNT}.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code remoteServerInfo == null}.
     * @throws NullPointerException
     *             if {@code localServerPorts == null}.
     */
    ConnectionToServer(final ServerInfo remoteServerInfo,
            final int[] localServerPorts) throws IOException {
        final int[] remoteServerPorts = remoteServerInfo.getPorts();

        if (remoteServerPorts.length != SOCKET_COUNT) {
            throw new IllegalArgumentException("remoteServerPorts.length: "
                    + remoteServerPorts.length);
        }
        if (localServerPorts.length != SOCKET_COUNT) {
            throw new IllegalArgumentException("localServerPorts.length: "
                    + localServerPorts.length);
        }
        try {
            final InetAddress serverAddress = remoteServerInfo.getInetAddress();
            final Socket[] sockets = new Socket[SOCKET_COUNT];
            for (int i = 0; i < SOCKET_COUNT; i++) {
                sockets[i] = new Socket(serverAddress, remoteServerPorts[i]);
                add(sockets[i]);
            }
            /*
             * For each socket:
             */
            for (final Socket socket : sockets) {
                /*
                 * Write the port numbers of the local server on the socket to
                 * help identify this client.
                 */
                final DataOutputStream stream = new DataOutputStream(
                        socket.getOutputStream());
                for (final int port : localServerPorts) {
                    stream.writeInt(port);
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
