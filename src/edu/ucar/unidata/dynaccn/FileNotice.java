/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;

/**
 * A notice of an available file.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class FileNotice extends Notice {
    /**
     * The serial version identifier.
     */
    private static final long serialVersionUID = 1L;

    /**
     * The associated information on the file.
     */
    private final FileInfo    fileInfo;

    /**
     * Constructs from information on the file.
     * 
     * @param fileInfo
     *            Information on the file
     * @throws NullPointerException
     *             if {@code fileInfo == null}.
     */
    FileNotice(final FileInfo fileInfo) {
        if (null == fileInfo) {
            throw new NullPointerException();
        }

        this.fileInfo = fileInfo;
    }

    @Override
    void process(final Peer peer) throws IOException {
        peer.createEmptyFile(fileInfo);
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.ucar.unidata.dynaccn.Notice#toString()
     */
    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{fileInfo=" + fileInfo + "}";
    }
}
