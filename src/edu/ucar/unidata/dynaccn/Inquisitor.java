/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.Socket;

import net.jcip.annotations.NotThreadSafe;

/**
 * Asks a {@link Tracker} what servers are available to satisfy a subscription
 * request.
 * 
 * Instances are thread-compatible but not thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@NotThreadSafe
final class Inquisitor implements Serializable, TrackerTask {
    /**
     * The serial version identifier.
     */
    private static final long serialVersionUID = 1L;
    /**
     * Information on the associated sink-node's server.
     */
    private final ServerInfo  sinkServerInfo;
    /**
     * The file-selection predicate.
     */
    private final Predicate   predicate;

    /**
     * Constructs from the predicate for selecting files and information on the
     * sink-node's server that will retransmit the files.
     * 
     * @param sinkServerInfo
     *            Information on the sink-node's server.
     * @param predicate
     *            The file-selection predicate.
     * @throws NullPointerException
     *             if {@code predicate == null || sinkServerInfo == null}.
     */
    Inquisitor(final ServerInfo sinkServerInfo, final Predicate predicate) {
        if (null == predicate || null == sinkServerInfo) {
            throw new NullPointerException();
        }
        this.sinkServerInfo = sinkServerInfo;
        this.predicate = predicate;
    }

    /**
     * Interacts with a {@link Tracker} to obtain information on servers that
     * can satisfy a data-request and to register the {@link SinkNode}'s server.
     * 
     * @param tracker
     *            The {@link Tracker} to use.
     * @param socket
     *            The connection to this instance's sink-node.
     * @throws IOException
     *             if an I/O error occurs.
     */
    public void process(final Tracker tracker, final Socket socket)
            throws IOException {
        final OutputStream outputStream = socket.getOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(outputStream);
        try {
            // TODO: vet the predicate against the source; reject if illegal
            final Connector connector = new Connector();
            tracker.informConnector(predicate, sinkServerInfo, connector);
            oos.writeObject(connector);
        }
        finally {
            try {
                oos.close();
            }
            catch (final IOException ignored) {
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Inquisitor [predicate=" + predicate + ", sinkServerInfo="
                + sinkServerInfo + "]";
    }

    private Object readResolve() {
        return new Inquisitor(sinkServerInfo, predicate);
    }
}
