/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Sends objects to the remote peer.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
abstract class Sender implements Callable<Void> {
    /**
     * The local peer.
     */
    protected final Peer peer;

    /**
     * Constructs from the local peer.
     * 
     * @param peer
     *            The local peer.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code peer == null}.
     */
    Sender(final Peer peer) {
        if (null == peer) {
            throw new NullPointerException();
        }

        this.peer = peer;
    }
}
