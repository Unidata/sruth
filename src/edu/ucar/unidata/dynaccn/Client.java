/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights reserved.
 * See file LICENSE in the top-level source directory for licensing information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connects to a remote server and exchanges data.
 * 
 * @author Steven R. Emmerson
 */
final class Client implements Callable<Void> {
    /**
     * The logging service.
     */
    private static final Logger logger = LoggerFactory.getLogger(Client.class);
    /**
     * The data clearing-house.
     */
    private final ClearingHouse clearingHouse;
    /**
     * Information on the remote server.
     */
    private final ServerInfo    serverInfo;

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
     * Returns information on the server to which this instance will connect or
     * has connected.
     * 
     * @return Information on the associated server.
     */
    ServerInfo getServerInfo() {
        return serverInfo;
    }

    /**
     * Executes this instance. Returns when either all data that can be received
     * has been received or an exception is thrown.
     * 
     * @throws AssertionError
     *             Shouldn't occur, but if it does, then the cause of the
     *             {@link AssertionError} will be the impossible exception.
     * @throws ConnectException
     *             if the connection to the remote server can't be made or is
     *             lost.
     * @throws Error
     *             if an {@link Error} is thrown.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if a serious I/O error occurs.
     * @throws RuntimeException
     *             if a {@link RuntimeException} is thrown.
     */
    @Override
    public Void call() throws ConnectException, IOException,
            InterruptedException {
        Thread.currentThread().setName(toString());
        final ConnectionToServer connection;
        try {
            connection = new ConnectionToServer(serverInfo);
        }
        catch (final IOException e) {
            throw (ConnectException) new ConnectException(
                    "Couldn't connect to " + serverInfo).initCause(e);
        }
        try {
            final Peer peer = new Peer(clearingHouse, connection);
            logger.debug("Peer starting: {}", connection);
            peer.call();
            logger.debug("Peer completed: {}", peer);
        }
        finally {
            connection.close();
        }
        return null;
    }

    /**
     * Returns the number of bytes downloaded since the later of the previous
     * call or the start of this client.
     * 
     * @return The number of downloaded bytes since last time or the start.
     */
    synchronized long getDownloadAmount() {
        // TODO
        return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Client [serverInfo=" + serverInfo + ",localPredicate="
                + clearingHouse.getPredicate() + "]";
    }
}
