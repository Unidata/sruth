/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;

import net.jcip.annotations.ThreadSafe;

/**
 * A notice of removed files.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class RemovedFilesNotice implements RemovalNotice {
    /**
     * Serial version ID.
     */
    private static final long serialVersionUID = 1L;
    /**
     * The specification of the removed files.
     */
    private final FileSetSpec files;

    /**
     * Constructs from a specification of the removed files.
     * 
     * @param files
     *            The specification of the removed files.
     * @throws NullPointerException
     *             if {@code files == null}.
     */
    RemovedFilesNotice(final FileSetSpec files) {
        if (null == files) {
            throw new NullPointerException();
        }
        this.files = files;
    }

    @Override
    public void processYourself(final Peer peer) throws IOException {
        for (final FileId fileId : files) {
            peer.remove(fileId);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "RemovedFilesNotice [files=" + files + "]";
    }
}
