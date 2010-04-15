/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Sends requests for data to the remote peer.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class RequestSender extends Sender {
    /**
     * Constructs from the local peer.
     * 
     * @param peer
     *            The local peer.
     */
    RequestSender(final Peer peer) {
        super(peer);
    }

    @Override
    public Void call() throws IOException, InterruptedException {
        final ObjectOutputStream stream = peer.getRequestOutputStream();

        for (PieceInfo pieceInfo = peer.getNextWantedPiece(); null != pieceInfo; pieceInfo = peer
                .getNextWantedPiece()) {
            final Request request = new Request(pieceInfo);

            System.out.println("Sending request: " + request);
            stream.writeObject(request);
            stream.flush();
        }

        return null;
    }
}
