/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved. See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Receives requests for data and acts upon them.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class NoticeReceiver extends Receiver<Notice> {
    /**
     * Constructs from the local peer.
     * 
     * @param peer
     *            The local peer.
     * @throws NullPointerException
     *             if {@code peer == null}.
     */
    NoticeReceiver(final Peer peer) {
        super(peer, Notice.class);
    }

    @Override
    protected ObjectInputStream getInputStream(final Peer peer)
            throws IOException {
        return peer.getNoticeInputStream();
    }

    @Override
    protected boolean process(final Notice notice) throws IOException {
        System.out.println("Received notice: " + notice);

        notice.process(peer);

        return !(notice instanceof DoneNotice);
    }
}
