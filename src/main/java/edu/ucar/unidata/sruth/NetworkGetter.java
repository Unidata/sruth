/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectStreamException;
import java.net.InetSocketAddress;
import java.net.Socket;

import net.jcip.annotations.ThreadSafe;

/**
 * Gets the current state of the network from a tracker and registers a node.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class NetworkGetter extends TrackerTask {
    /**
     * The serial version identifier.
     */
    private static final long       serialVersionUID = 1L;
    /**
     * The data-selection filter.
     * 
     * @serial
     */
    private final Filter            filter;
    /**
     * The address of the server of the node that wants data.
     * 
     * @serial
     */
    private final InetSocketAddress localServer;

    /**
     * Constructs from the data-filter to use and the address of the local
     * server.
     * 
     * @param filter
     *            The data-filter to use.
     * @param localServer
     *            The address of the local server.
     * @throws NullPointerException
     *             if {@code filter == null}.
     * @throws NullPointerException
     *             if {@code localServer == null}.
     */
    NetworkGetter(final Filter filter, final InetSocketAddress localServer) {
        if (filter == null) {
            throw new NullPointerException();
        }
        if (localServer == null) {
            throw new NullPointerException();
        }
        this.filter = filter;
        this.localServer = localServer;
    }

    /**
     * Returns the data-filter.
     * 
     * @return the data-filter.
     */
    Filter getFilter() {
        return filter;
    }

    /**
     * Gets the state of the network from a tracker and registers with the
     * tracker.
     * <p>
     * This method is uninterruptible and potentially slow.
     * 
     * @param socket
     *            The client socket to the tracker.
     * @throws ClassCastException
     *             if the tracker returns the wrong type.
     * @throws ClassNotFoundException
     *             if the tracker's response is invalid.
     * @throws IOException
     *             if an I/O error occurs.
     */
    FilterServerMap getNetworkAndRegister(final Socket socket)
            throws ClassNotFoundException, IOException {
        return (FilterServerMap) callTracker(socket);
    }

    @Override
    public void process(final Tracker tracker, final Socket socket)
            throws IOException {
        final FilterServerMap network = tracker.getNetwork(filter);
        reply(socket, network);
        socket.close();
        tracker.register(localServer, filter);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "NetworkGetter [filter=" + filter + ",localServer="
                + localServer + "]";
    }

    private Object readResolve() throws ObjectStreamException {
        try {
            return new NetworkGetter(filter, localServer);
        }
        catch (final NullPointerException e) {
            throw (InvalidObjectException) new InvalidObjectException(
                    "Null response from tracker").initCause(e);
        }
    }
}
