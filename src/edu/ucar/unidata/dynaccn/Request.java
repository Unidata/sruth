/**
 * 
 */
package edu.ucar.unidata.dynaccn;

import java.io.InvalidObjectException;
import java.io.Serializable;

/**
 * A request for a piece of data.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class Request implements Serializable {
    /**
     * The serial version identifier.
     */
    private static final long serialVersionUID = 1L;
    /**
     * Information on the piece of data being requested.
     */
    private final PieceInfo   pieceInfo;

    /**
     * Creates a request for a piece of data.
     * 
     * @param pieceInfo
     *            Information on the piece of data to request.
     * @throws NullPointerException
     *             if {@code pieceInfo == null}.
     */
    Request(final PieceInfo pieceInfo) {
        if (null == pieceInfo) {
            throw new NullPointerException();
        }

        this.pieceInfo = pieceInfo;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    private Object readResolve() throws InvalidObjectException {
        try {
            return new Request(pieceInfo);
        }
        catch (final Exception e) {
            throw (InvalidObjectException) new InvalidObjectException(
                    "Read invalid " + getClass().getSimpleName()).initCause(e);
        }
    }
}
