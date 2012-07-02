/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source-directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;

import net.jcip.annotations.ThreadSafe;

/**
 * A request for notices of data-pieces.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class NoticeRequest implements Request {
    /**
     * The serial version identifier.
     */
    private static final long       serialVersionUID = 1L;
    /**
     * The set of data-piece specifications to consider for notices.
     */
    private final PieceSpecSetIface specs;

    /**
     * Constructs from a set of data-piece specifications.
     * 
     * @param specs
     *            The set of data-piece specifications. The client shall not
     *            modify.
     * @throws NullPointerException
     *             if {@code specs == null}.
     */
    NoticeRequest(final PieceSpecSetIface specs) {
        if (null == specs) {
            throw new NullPointerException();
        }
        this.specs = specs;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.ucar.unidata.sruth.PeerMessage#processYourself(edu.ucar.unidata.sruth
     * .Peer)
     */
    @Override
    public void processYourself(final Peer peer) throws IOException,
            InterruptedException {
        peer.queueForSendingNotices(specs);
    }
}
