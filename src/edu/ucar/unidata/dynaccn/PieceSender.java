/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Sends pieces of data to the remote peer.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class PieceSender extends Sender {
    /**
     * Constructs from the local peer.
     * 
     * @param peer
     *            The local peer.
     */
    PieceSender(final Peer peer) {
        super(peer);
    }

    @Override
    public Void call() throws IOException, InterruptedException {
        final ObjectOutputStream stream = peer.getDataOutputStream();

        for (Piece piece = peer.getNextPiece(); null != piece; piece = peer
                .getNextPiece()) {
            System.out.println("Sending piece: " + piece);
            stream.writeObject(piece);
            stream.flush();
        }

        return null;
    }
}
