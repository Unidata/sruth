/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.InvalidObjectException;
import java.util.Arrays;

import edu.ucar.unidata.sruth.Connection.Message;

/**
 * A piece of a file.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class Piece implements Message {
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
     * Returns the amount of data in this instance in octets.
     * 
     * @return The amount of data in this instance.
     */
    int getSize() {
        return pieceSpec.getSize();
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
     * Returns the archive pathname of this instance.
     * 
     * @return The archive pathname of this instance.
     */
    ArchivePath getArchivePath() {
        return pieceSpec.getArchivePath();
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

    /**
     * Returns the time-to-live, in seconds, of the associated file.
     * 
     * @return The time-to-live, in seconds, of the associated file.
     */
    int getTimeToLive() {
        return pieceSpec.getTimeToLive();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(data);
        result = prime * result + ((pieceSpec == null)
                ? 0
                : pieceSpec.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Piece other = (Piece) obj;
        if (!Arrays.equals(data, other.data)) {
            return false;
        }
        if (pieceSpec == null) {
            if (other.pieceSpec != null) {
                return false;
            }
        }
        else if (!pieceSpec.equals(other.pieceSpec)) {
            return false;
        }
        return true;
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
