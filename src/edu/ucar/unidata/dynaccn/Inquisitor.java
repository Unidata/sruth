/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import net.jcip.annotations.NotThreadSafe;

/**
 * Asks a {@link Tracker} what nodes are available.
 * 
 * Instances are thread-compatible but not thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@NotThreadSafe
final class Inquisitor implements Serializable {
    /**
     * The serial version identifier.
     */
    private static final long serialVersionUID = 1L;
    /**
     * Information on the associated server.
     */
    private final ServerInfo  serverInfo;
    /**
     * The file-selection predicate.
     */
    private final Predicate   predicate;

    /**
     * Constructs from the predicate for selecting files and information on the
     * server that will be available to retransmit the files.
     * 
     * @param serverInfo
     *            Information on the associated server.
     * @param predicate
     *            The file-selection predicate.
     * @throws NullPointerException
     *             if {@code predicate == null || serverInfo == null}.
     */
    Inquisitor(final ServerInfo serverInfo, final Predicate predicate) {
        if (null == predicate || null == serverInfo) {
            throw new NullPointerException();
        }
        this.serverInfo = serverInfo;
        this.predicate = predicate;
    }

    /**
     * Interacts with a {@link Tracker} and with its remote node to obtain
     * server information and to register its associated server.
     * 
     * @param tracker
     *            The {@link Tracker} to use.
     * @param oos
     *            The connection to this instance's remote node.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void process(final Tracker tracker, final ObjectOutputStream oos)
            throws IOException {
        // TODO: vet the predicate against the source; reject if illegal
        final Plumber plumber = tracker.getPlumber(predicate);
        oos.writeObject(plumber);
        tracker.register(serverInfo, predicate);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Inquisitor [predicate=" + predicate + ", serverInfo="
                + serverInfo + "]";
    }

    private Object readResolve() {
        return new Inquisitor(serverInfo, predicate);
    }
}
