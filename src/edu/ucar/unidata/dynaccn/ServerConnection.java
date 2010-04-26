/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.net.Socket;

/**
 * The server-side of a connection to a client.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class ServerConnection extends Connection {
    /*
     * (non-Javadoc)
     * 
     * @see edu.ucar.unidata.dynaccn.Connection#getServerPort(java.net.Socket)
     */
    @Override
    protected int getServerPort(final Socket socket) {
        return socket.getLocalPort();
    }
}
