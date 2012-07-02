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
     * Archive-pathname of the removed file.
     */
    private final ArchivePath archivePath;

    /**
     * Constructs from the archive-pathname of the removed file.
     * 
     * @param archivePath
     *            Archive-pathname of the removed file.
     * @throws NullPointerException
     *             if {@code archivePath == null}.
     */
    RemovedFileNotice(final ArchivePath archivePath) {
        if (null == archivePath) {
            throw new NullPointerException();
        }
        this.archivePath = archivePath;
    }

    @Override
    public void processYourself(final Peer peer) throws IOException {
        peer.remove(archivePath);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "RemovedFileNotice [archivePath=" + archivePath + "]";
    }

    private Object readResolve() {
        return new RemovedFileNotice(archivePath);
    }
}
