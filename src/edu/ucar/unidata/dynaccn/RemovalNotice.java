/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;

import net.jcip.annotations.ThreadSafe;

/**
 * A notice of removed files.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
interface RemovalNotice extends Notice {
    /**
     * Processes the file specifications of this instance by means of a local
     * peer.
     * 
     * @param peer
     *            The local peer.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void processYourself(final Peer peer) throws IOException;
}
