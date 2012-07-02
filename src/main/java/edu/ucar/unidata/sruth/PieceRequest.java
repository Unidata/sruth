/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;

import net.jcip.annotations.ThreadSafe;

/**
 * A request for pieces of data.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class PieceRequest implements Request {
    /**
     * The serial version ID.
     */
    private static final long       serialVersionUID = 1L;
    /**
     * The set of data-piece specifications.
     */
    private final PieceSpecSetIface set;

    /**
     * Constructs from a set of data-piece specifications.
     * 
     * @param set
     *            The set of data-piece specifications.
     * @throws NullPointerException
     *             if {@code set == null}.
     */
    PieceRequest(final PieceSpecSetIface set) {
        if (null == set) {
            throw new NullPointerException();
        }
        this.set = set;
    }

    @Override
    public void processYourself(final Peer peer) throws IOException,
            InterruptedException {
        peer.queueForSending(set);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "PieceRequest [set=" + set + "]";
    }
}
