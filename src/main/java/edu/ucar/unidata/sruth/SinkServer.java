/**
 * Copyright 2011 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketException;

/**
 * The server for sink-nodes.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class SinkServer extends Server {
    /**
     * Constructs from the data-exchange clearing-house. Immediately starts
     * listening for connection attempts but doesn't process the attempts until
     * method {@link #call()} is called. This constructor calls
     * {@link Server#Server(ClearingHouse)}.
     * 
     * @param clearingHouse
     *            The data-exchange clearing-house.
     * @throws IOException
     *             if a port can't be listened to.
     * @throws NullPointerException
     *             if {@code clearingHouse == null}.
     * @see Server#Server(ClearingHouse)
     */
    SinkServer(final ClearingHouse clearingHouse) throws IOException {
        super(clearingHouse);
    }

    /**
     * Constructs from the data clearing-house that the server should use and
     * the set of port numbers that the server could use.
     * 
     * @param clearingHouse
     *            The data clearing-house that the server should use.
     * @param inetSockAddrSet
     *            The set of candidate Internet socket addresses for the server.
     * @throws IOException
     *             If an I/O error occurs.
     * @throws NullPointerException
     *             if {@code clearingHouse == null}.
     * @throws NullPointerException
     *             if {@code inetSockAddrSet == null}.
     * @throws SocketException
     *             if a socket couldn't be created.
     */
    SinkServer(final ClearingHouse clearingHouse,
            final InetSocketAddressSet inetSockAddrSet) throws IOException,
            SocketException {
        super(clearingHouse, inetSockAddrSet);
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.ucar.unidata.sruth.Server#createSocket()
     */
    @Override
    void adjustSocket(final ServerSocket socket) {
        /*
         * This implementation does nothing.
         */
    }
}
