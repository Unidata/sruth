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
    private final ServerInfo    remoteServerInfo;
    /**
     * The port numbers of the associated local server.
     */
    private final int[]         localServerPorts;

    /**
     * Constructs from information on the remote server, the port numbers of the
     * local server, and the clearing house to be used.
     * 
     * @param remoteServerInfo
     *            Information on the remote server.
     * @param localServerPorts
     *            Port numbers of the associated local server. EACH PORT NUMBER
     *            MUST BE UNIQUE TO THIS INSTANCE.
     * @param clearingHouse
     *            The clearing-house to use.
     * @throws NullPointerException
     *             if
     *             {@code remoteServerInfo == null || localServerPorts == null || clearingHouse == null}
     *             .
     */
    Client(final ServerInfo remoteServerInfo, final int[] localServerPorts,
            final ClearingHouse clearingHouse) {
        if (null == clearingHouse || null == remoteServerInfo) {
            throw new NullPointerException();
        }

        this.clearingHouse = clearingHouse;
        this.remoteServerInfo = remoteServerInfo;
        this.localServerPorts = localServerPorts.clone();
    }

    /**
     * Returns information on the remote server to which this instance will
     * connect or has connected.
     * 
     * @return Information on the remote server.
     */
    ServerInfo getServerInfo() {
        return remoteServerInfo;
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
            connection = new ConnectionToServer(remoteServerInfo,
                    localServerPorts);
        }
        catch (final IOException e) {
            throw (ConnectException) new ConnectException(
                    "Couldn't connect to " + remoteServerInfo).initCause(e);
        }
        try {
            final Peer peer = new Peer(clearingHouse, connection);
            logger.debug("Starting up: {}", peer);
            try {
                peer.call();
            }
            finally {
                logger.debug("Terminated: {}", peer);
            }
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
        return "Client [remoteServerInfo=" + remoteServerInfo
                + ",localPredicate=" + clearingHouse.getPredicate() + "]";
    }
}
