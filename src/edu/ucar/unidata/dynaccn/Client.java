/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights reserved.
 * See file LICENSE in the top-level source directory for licensing information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

/**
 * Connects to a remote server and exchanges data.
 * 
 * @author Steven R. Emmerson
 */
final class Client implements Callable<Void> {
    /**
     * The logging service.
     */
    private static final Logger    logger     = Logger.getLogger(Client.class
                                                      .getName());
    /**
     * The connection to the remote server.
     */
    private final ClientConnection connection = new ClientConnection();
    /**
     * The data clearing-house.
     */
    private final ClearingHouse    clearingHouse;
    /**
     * Information on the remote server.
     */
    private final ServerInfo       serverInfo;

    /**
     * Constructs from information on the remote server.
     * 
     * @param serverInfo
     *            Information on the remote server.
     * @param clearingHouse
     *            The clearing-house to use.
     * @throws NullPointerException
     *             if {@code serverInfo == null || clearingHouse == null}.
     */
    Client(final ServerInfo serverInfo, final ClearingHouse clearingHouse) {
        if (null == clearingHouse || null == serverInfo) {
            throw new NullPointerException();
        }

        this.clearingHouse = clearingHouse;
        this.serverInfo = serverInfo;
    }

    /**
     * Executes this instance and waits upon one of the following conditions: 1)
     * all data that can be received has been received; 2) an error occurs; or
     * 3) the current thread is interrupted. In any case, any and all subtasks
     * will have been terminated upon return.
     * 
     * @throws IOException
     *             if an I/O error occurs while attempting to connect to the
     *             remote server.
     * @throws ExecutionException
     *             if this instance terminated due to an error.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    @Override
    public Void call() throws IOException, InterruptedException,
            ExecutionException {
        final int[] serverPorts = serverInfo.getPorts();
        final InetAddress serverAddress = serverInfo.getInetAddress();
        final Socket[] sockets = new Socket[serverPorts.length];
        /*
         * Create this client's sockets and get their port numbers.
         */
        for (int i = 0; i < sockets.length; i++) {
            sockets[i] = new Socket(); // unbound socket
            sockets[i].bind(null); // obtains ephemeral port
        }
        /*
         * For each socket:
         */
        for (int i = 0; i < sockets.length; i++) {
            /*
             * Connect the socket to the server.
             */
            sockets[i].connect(new InetSocketAddress(serverAddress,
                    serverPorts[i]));
            /*
             * Write this client's port numbers on the socket to help identify
             * this client.
             */
            final DataOutputStream stream = new DataOutputStream(sockets[i]
                    .getOutputStream());
            for (final Socket socket : sockets) {
                stream.writeInt(socket.getLocalPort());
            }
            stream.flush();
            /*
             * Add this socket to the connection.
             */
            connection.add(sockets[i]);
        }
        logger.finer("Client: " + connection);
        return new Peer(clearingHouse, connection).call();
    }
}
