/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * A connection to a client.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class ConnectionToClient extends Connection {
    /**
     * Reads the port-numbers from a socket that are written by a client. The
     * port-numbers are those of the server associated with the client.
     * 
     * @param socket
     *            The socket that's connected to the client. Must have just come
     *            from a {@link java.net.ServerSocket#accept()}.
     * @throws IOException
     *             if an I/O error occurs.
     */
    static int[] getRemoteServerPorts(final Socket socket) throws IOException {
        /*
         * Get the port numbers of the client.
         */
        final DataInputStream dis = new DataInputStream(socket.getInputStream());
        final int[] serverPorts = new int[SOCKET_COUNT];
        for (int i = 0; i < serverPorts.length; i++) {
            serverPorts[i] = dis.readInt();
        }
        return serverPorts;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.ucar.unidata.dynaccn.Connection#getServerPort(java.net.Socket)
     */
    @Override
    protected int getServerPort(final Socket socket) {
        return socket.getLocalPort();
    }
}
