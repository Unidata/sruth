/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved. See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Receives pieces of data and acts upon them.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class PieceReceiver extends Receiver<Piece> {
    /**
     * Constructs from the local peer.
     * 
     * @param peer
     *            The local peer.
     * @throws NullPointerException
     *             if {@code peer == null}.
     */
    PieceReceiver(final Peer peer) {
        super(peer, Piece.class);
    }

    @Override
    protected ObjectInputStream getInputStream(final Peer peer)
            throws IOException {
        return peer.getDataInputStream();
    }

    @Override
    protected boolean process(final Piece piece) throws IOException,
            InterruptedException {
        System.out.println("Received piece: " + piece);

        return !peer.processData(piece);
    }
}
