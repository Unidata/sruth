/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

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
     * Identifier of the removed file.
     */
    private final FileId      fileId;

    /**
     * Constructs from a specification of the removed file.
     * 
     * @param fileId
     *            Identifier of the removed file.
     * @throws NullPointerException
     *             if {@code fileId == null}.
     */
    RemovedFileNotice(final FileId fileId) {
        if (null == fileId) {
            throw new NullPointerException();
        }
        this.fileId = fileId;
    }

    @Override
    public void processYourself(final Peer peer) throws IOException {
        peer.remove(fileId);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "RemovedFileNotice [fileId=" + fileId + "]";
    }
}
