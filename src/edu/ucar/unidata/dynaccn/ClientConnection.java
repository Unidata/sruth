/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.net.Socket;

/**
 * The client-side of a connection to a server.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class ClientConnection extends Connection {
    /*
     * (non-Javadoc)
     * 
     * @see edu.ucar.unidata.dynaccn.Connection#getServerPort(java.net.Socket)
     */
    @Override
    protected int getServerPort(final Socket socket) {
        return socket.getPort();
    }
}
