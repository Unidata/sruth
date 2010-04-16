/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Sends notices of available data to a remote peer.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class NoticeSender extends Sender {
    /**
     * Constructs from the local peer and a directory pathname.
     * 
     * @param peer
     *            The local peer.
     * @param dirPath
     *            Pathname of the directory that contains the potential files to
     *            be sent.
     * @throws NullPointerException
     *             if {@code peer == null || dirPath == null}.
     */
    NoticeSender(final Peer peer, final File dirPath) throws IOException {
        super(peer);

        if (null == dirPath) {
            throw new NullPointerException();
        }
    }

    @Override
    public Void call() throws IOException {
        final ObjectOutputStream objStream = peer.getNoticeOutputStream();

        for (Notice notice = peer.getNextNotice(); null != notice; notice = peer
                .getNextNotice()) {
            System.out.println("Sending notice: " + notice);
            objStream.writeObject(notice);
            objStream.flush();
        }

        return null;
    }
}
