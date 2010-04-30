package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.Serializable;

/**
 * An object on the request-stream.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
abstract class Request implements Serializable {
    /**
     * Serial version ID.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Processes the request via the local peer.
     * 
     * @param peer
     *            The local peer.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if an I/O error occurs.
     */
    abstract void process(final Peer peer) throws InterruptedException,
            IOException;
}