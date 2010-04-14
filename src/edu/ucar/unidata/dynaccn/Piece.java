/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.Serializable;

/**
 * A piece of a file.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class Piece implements Serializable {
    /**
     * The serial version identifier.
     */
    private static final long serialVersionUID = 1L;
    /**
     * Information on the piece.
     */
    private final PieceInfo   pieceInfo;
    /**
     * The piece's data.
     */
    private final byte[]      data;

    /**
     * Constructs from information on the piece and the piece's data.
     * 
     * @param pieceInfo
     *            Information on the piece.
     * @param data
     *            The piece's data. NB: Not copied.
     * @throws IllegalArgumentException
     *             if {@code data} has the wrong number of elements.
     * @throws NullPointerException
     *             if {@code pieceInfo == null}.
     * @throws NullPointerException
     *             if {@code data == null}.
     */
    Piece(final PieceInfo pieceInfo, final byte[] data) {
        pieceInfo.vet(data);

        this.pieceInfo = pieceInfo;
        this.data = data;
    }

    /**
     * Returns the associated piece-information.
     * 
     * @return The associated piece-information.
     */
    PieceInfo getInfo() {
        return pieceInfo;
    }

    /**
     * Returns the piece-index of this instance.
     * 
     * @return The piece-index of this instance.
     */
    long getIndex() {
        return pieceInfo.getIndex();
    }

    /**
     * Returns this instance's data.
     * 
     * @return This instance's data. NB: Not copied.
     */
    byte[] getData() {
        return data;
    }
}
