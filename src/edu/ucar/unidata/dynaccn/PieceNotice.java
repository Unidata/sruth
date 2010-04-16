/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.InvalidObjectException;

/**
 * A notice of an available piece of data.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class PieceNotice extends Notice {
    /**
     * The serial version identifier.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Information about the associated file-piece.
     */
    private final PieceInfo   pieceInfo;

    /**
     * Constructs from information about a file-piece.
     * 
     * @param pieceInfo
     *            Information of the file-piece.
     * @throws NullPointerException
     *             if {@code pieceInfo == null}.
     */
    PieceNotice(final PieceInfo pieceInfo) {
        if (null == pieceInfo) {
            throw new NullPointerException();
        }

        this.pieceInfo = pieceInfo;
    }

    @Override
    void process(final Peer peer) throws InterruptedException {
        peer.processNotice(pieceInfo);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{pieceInfo=" + pieceInfo + "}";
    }

    private Object readResolve() throws InvalidObjectException {
        try {
            return new PieceNotice(pieceInfo);
        }
        catch (final Exception e) {
            throw (InvalidObjectException) new InvalidObjectException(
                    "Read invalid " + getClass().getSimpleName()).initCause(e);
        }
    }
}
