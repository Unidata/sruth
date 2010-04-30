package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.InvalidObjectException;

/**
 * A request for a piece of data.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class PieceRequest extends Request {
    /**
     * Serial version ID.
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
    PieceRequest(final PieceInfo pieceInfo) {
        if (null == pieceInfo) {
            throw new NullPointerException();
        }

        this.pieceInfo = pieceInfo;
    }

    /**
     * Processes the associated piece-information via the local peer.
     * 
     * @param peer
     *            The local peer.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if an I/O error occurs.
     */
    @Override
    void process(final Peer peer) throws InterruptedException, IOException {
        peer.process(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{pieceInfo=" + pieceInfo + "}";
    }

    private Object readResolve() throws InvalidObjectException {
        try {
            return new PieceRequest(pieceInfo);
        }
        catch (final Exception e) {
            throw (InvalidObjectException) new InvalidObjectException(
                    "Read invalid " + getClass().getSimpleName())
                    .initCause(e);
        }
    }

    /**
     * Returns information on the piece of data.
     * 
     * @return Information on the piece of data.
     */
    PieceInfo getPieceInfo() {
        return pieceInfo;
    }
}