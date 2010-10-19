/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.net.Socket;

/**
 * A task that uses a tracker to accomplish itself.
 * 
 * @author Steven R. Emmerson
 */
interface TrackerTask {
    /**
     * Interacts with a {@link Tracker} to accomplish its mission.
     * 
     * @param tracker
     *            The {@link Tracker} to use.
     * @param socket
     *            The connection to the originating node if a reply is expected.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void process(final Tracker tracker, final Socket socket) throws IOException;
}
