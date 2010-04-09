/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights reserved.
 * See file LICENSE in the top-level source directory for licensing information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

/**
 * Connects to a remote server and exchanges data.
 * 
 * @author Steven R. Emmerson
 */
final class Client {
    /**
     * The connection to the remote server.
     */
    private final Connection connection = new Connection();

    /**
     * Constructs from the Internet address of the remote server. Executes
     * immediately. NOTE: Connections are made to the server in sequence from
     * the lowest port number to the highest.
     * 
     * @param inetAddress
     *            The Internet address of the remote server.
     * @throws IOException
     *             if an I/O error occurs while attempting to connect to the
     *             remote server.
     * @throws NullPointerException
     *             if {@code inetAddress} is {@code null}.
     */
    Client(final InetAddress inetAddress) throws IOException {
        for (int i = 0; i < Connection.SOCKET_COUNT; i++) {
            final int port = Server.START_PORT + i;

            try {
                connection.add(new Socket(inetAddress, port));
            }
            catch (final IOException e) {
                connection.close();
                throw (IOException) new IOException("Couldn't connect to port "
                        + port + " on host " + inetAddress).initCause(e);
            }
        }
        RequestSender.start(connection.getOutputRequestStream());
    }
}
