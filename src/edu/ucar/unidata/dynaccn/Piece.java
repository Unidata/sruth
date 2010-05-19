/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.InvalidObjectException;
import java.io.Serializable;
import java.nio.file.Path;

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
    private final PieceSpec   pieceSpec;
    /**
     * The piece's data.
     */
    private final byte[]      data;

    /**
     * Constructs from information on the piece and the piece's data.
     * 
     * @param pieceSpec
     *            Information on the piece.
     * @param data
     *            The piece's data. NB: Not copied.
     * @throws IllegalArgumentException
     *             if {@code data} has the wrong number of elements.
     * @throws NullPointerException
     *             if {@code pieceSpec == null}.
     * @throws NullPointerException
     *             if {@code data == null}.
     */
    Piece(final PieceSpec pieceSpec, final byte[] data) {
        pieceSpec.vet(data);

        this.pieceSpec = pieceSpec;
        this.data = data;
    }

    /**
     * Returns the associated piece-information.
     * 
     * @return The associated piece-information.
     */
    PieceSpec getInfo() {
        return pieceSpec;
    }

    /**
     * Returns the piece-index of this instance.
     * 
     * @return The piece-index of this instance.
     */
    int getIndex() {
        return pieceSpec.getIndex();
    }

    /**
     * Returns this instance's data.
     * 
     * @return This instance's data. NB: Not copied.
     */
    byte[] getData() {
        return data;
    }

    /**
     * Returns information on the file that contains this piece of data.
     * 
     * @return Information on the containing file.
     */
    FileInfo getFileInfo() {
        return pieceSpec.getFileInfo();
    }

    /**
     * Returns the relative pathname of this instance.
     * 
     * @return The relative pathname of this instance.
     */
    Path getPath() {
        return pieceSpec.getPath();
    }

    /**
     * Returns the number of bytes from the beginning of the file to the start
     * of this piece.
     * 
     * @return The offset, in bytes, to the start of this piece.
     */
    long getOffset() {
        return pieceSpec.getOffset();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{pieceSpec=" + pieceSpec
                + ", size=" + data.length + "}";
    }

    private Object readResolve() throws InvalidObjectException {
        try {
            return new Piece(pieceSpec, data);
        }
        catch (final Exception e) {
            throw (InvalidObjectException) new InvalidObjectException(
                    "Read invalid " + getClass().getSimpleName()).initCause(e);
        }
    }
}
