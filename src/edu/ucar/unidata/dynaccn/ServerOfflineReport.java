/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.Serializable;
import java.net.Socket;

import net.jcip.annotations.NotThreadSafe;

/**
 * Asks a {@link Tracker} what nodes are available.
 * 
 * Instances are thread-compatible but not thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@NotThreadSafe
final class ServerOfflineReport implements Serializable, TrackerTask {
    /**
     * The serial version identifier.
     */
    private static final long serialVersionUID = 1L;
    /**
     * Information on the offline localServer.
     */
    private final ServerInfo  serverInfo;

    /**
     * Constructs from information on the offline localServer.
     * 
     * @param serverInfo
     *            Information on the offline localServer.
     * @throws NullPointerException
     *             if {@code serverInfo == null}.
     */
    ServerOfflineReport(final ServerInfo serverInfo) {
        if (null == serverInfo) {
            throw new NullPointerException();
        }
        this.serverInfo = serverInfo;
    }

    /**
     * Interacts with a {@link Tracker} to handle a localServer going offline.
     * 
     * @param tracker
     *            The {@link Tracker} to use.
     * @param socket
     *            The connection to originating node.
     * @throws IOException
     *             if an I/O error occurs.
     */
    public void process(final Tracker tracker, final Socket socket) {
        tracker.serverOffline(serverInfo);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "ServerOfflineReport [serverInfo=" + serverInfo + "]";
    }
}
