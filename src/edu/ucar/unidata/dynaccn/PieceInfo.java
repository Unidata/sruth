/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.Serializable;

/**
 * Information about a piece of a file.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class PieceInfo implements Serializable {
    /**
     * The serial version identifier.
     */
    private static final long serialVersionUID = 1L;
    /**
     * Information on the file.
     */
    private final FileInfo    fileInfo;
    /**
     * The piece index.
     */
    private final long        index;

    /**
     * Constructs from a file identifier and a piece index.
     * 
     * @param fileInfo
     *            Information on the file.
     * @param index
     *            The piece index.
     * @throws IllegalArgumentException
     *             if {@code index} lies outside the valid range.
     * @throws NullPointerException
     *             if {@code fileInfo == null}.
     */
    PieceInfo(final FileInfo fileInfo, final long index) {
        fileInfo.vet(index);

        this.fileInfo = fileInfo;
        this.index = index;
    }

    /**
     * Vets an array of data.
     * 
     * @param data
     *            The data to be vetted.
     * @throws IllegalArgumentException
     *             if {@code data} has the wrong number of elements.
     * @throws NullPointerException
     *             if {@code data == null}.
     */
    void vet(final byte[] data) {
        fileInfo.vet(index, data);
    }

    /**
     * Returns the file-information associated with this instance.
     * 
     * @return The associated file-information.
     */
    FileInfo getFileInfo() {
        return fileInfo;
    }

    /**
     * Returns the piece-index of this instance.
     * 
     * @return The piece-index of this instance.
     */
    long getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{fileInfo=" + fileInfo
                + ", index=" + index + "}";
    }
}
