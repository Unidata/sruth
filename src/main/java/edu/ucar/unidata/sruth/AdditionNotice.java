/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;

import net.jcip.annotations.ThreadSafe;

/**
 * A notice of new data.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class AdditionNotice implements Notice {
    /**
     * The serial version ID.
     */
    private static final long  serialVersionUID = 1L;
    /**
     * The specification of the new data.
     */
    private final PieceSpecSet set;

    /**
     * Constructs from a set of piece-specifications.
     * 
     * @param set
     *            The set of piece-specifications.
     * @throws NullPointerException
     *             if {@code set == null}.
     */
    AdditionNotice(final PieceSpecSet set) {
        if (null == set) {
            throw new NullPointerException();
        }
        this.set = set;
    }

    @Override
    public void processYourself(final Peer peer) throws IOException,
            InterruptedException {
        for (final PieceSpec spec : set) {
            peer.newRemoteData(spec);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "AdditionNotice [set=" + set + "]";
    }
}
