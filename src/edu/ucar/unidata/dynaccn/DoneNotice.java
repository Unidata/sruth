/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

/**
 * A notice that indicates that no more notices will be forthcoming.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class DoneNotice extends Notice {
    /**
     * The serial version identifier.
     */
    private static final long serialVersionUID = 1L;

    /**
     * The single instance of this class.
     */
    static final DoneNotice   INSTANCE         = new DoneNotice();

    private DoneNotice() {
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.ucar.unidata.dynaccn.Notice#toString()
     */
    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    @Override
    void process(final Peer peer) {
    }
}
