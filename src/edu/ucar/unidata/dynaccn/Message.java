/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

/**
 * Messages that are exchanged between peers.
 * 
 * @author Steven R. Emmerson
 */
interface Message {
    /**
     * Causes this instance to process itself via a peer.
     * 
     * @param peer
     *            The peer to use for processing.
     */
    void processYourself(Peer peer);
}
