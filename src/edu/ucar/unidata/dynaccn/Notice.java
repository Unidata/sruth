package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.Serializable;

/**
 * A notice of available data.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
abstract class Notice implements Serializable {
    /**
     * The serial version identifier.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Process this instance using the local peer.
     * 
     * @param peer
     *            The local peer.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    abstract void process(Peer peer) throws IOException, InterruptedException;

    @Override
    public abstract String toString();
}