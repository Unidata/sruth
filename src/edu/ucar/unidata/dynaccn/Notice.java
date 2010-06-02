/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.Serializable;

/**
 * The interface for objects that are exchanged on the NOTICE line.
 * 
 * @author Steven R. Emmerson
 */
interface Notice extends Serializable {
    /**
     * Has this instance process itself using the local peer.
     * 
     * @param peer
     *            The local peer.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    void processYourself(Peer peer) throws IOException, InterruptedException;
}
