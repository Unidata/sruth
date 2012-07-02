package edu.ucar.unidata.sruth;

import java.io.IOException;

import edu.ucar.unidata.sruth.Connection.Message;

/**
 * Interface for messages exchanged by peers.
 * 
 * @author Steven R. Emmerson
 */
interface PeerMessage extends Message {
    /**
     * Have this instance process itself using the local peer.
     * 
     * @param peer
     *            The local peer.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    void processYourself(Peer peer) throws IOException,
            InterruptedException;
}