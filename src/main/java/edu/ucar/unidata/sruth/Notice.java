/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;

import edu.ucar.unidata.sruth.Connection.Message;

/**
 * The interface for objects that are exchanged on the NOTICE line.
 * 
 * @author Steven R. Emmerson
 */
interface Notice extends Message {
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
