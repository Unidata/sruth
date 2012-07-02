/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;

import net.jcip.annotations.ThreadSafe;

/**
 * A notice of removed archivePaths.
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
    private static final long    serialVersionUID = 1L;
    /**
     * The archive-pathnames of the removed archivePaths.
     */
    private final ArchivePathSet archivePaths;

    /**
     * Constructs from the pathnames of the removed archive-files.
     * 
     * @param archivePaths
     *            The pathnames of the removed archive-files.
     * @throws NullPointerException
     *             if {@code archivePaths == null}.
     */
    RemovedFilesNotice(final ArchivePathSet archivePaths) {
        if (null == archivePaths) {
            throw new NullPointerException();
        }
        this.archivePaths = archivePaths;
    }

    @Override
    public void processYourself(final Peer peer) throws IOException {
        for (final ArchivePath archivePath : archivePaths) {
            peer.remove(archivePath);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "RemovedFilesNotice [archivePaths=" + archivePaths + "]";
    }
}
