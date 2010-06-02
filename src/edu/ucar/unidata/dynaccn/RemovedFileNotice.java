/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;

/**
 * Notice of a removed file or category.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class RemovedFileNotice implements RemovalNotice {
    /**
     * The serial version ID.
     */
    private static final long serialVersionUID = 1L;
    /**
     * Specification of the removed file.
     */
    private final FileId      spec;

    /**
     * Constructs from a specification of the removed file.
     * 
     * @param spec
     *            Specification of the removed file.
     * @throws NullPointerException
     *             if {@code spec == null}.
     */
    RemovedFileNotice(final FileId spec) {
        if (null == spec) {
            throw new NullPointerException();
        }
        this.spec = spec;
    }

    @Override
    public void processYourself(final Peer peer) throws IOException {
        peer.remove(spec);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "RemovedFileNotice [spec=" + spec + "]";
    }
}
