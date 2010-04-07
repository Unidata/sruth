package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.net.Socket;

/**
 * A set of sockets that comprise a connection.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class SocketSet {
    static final int       COMPLETE_COUNT = 3;
    private final Socket[] sockets        = new Socket[COMPLETE_COUNT];
    private int            count          = 0;

    /**
     * Adds a socket to the set.
     * 
     * @param socket
     *            The socket to be added.
     * @throws IndexOutOfBoundsException
     *             if the set of sockets is already complete.
     * @throws NullPointerException
     *             if {@code socket} is {@code null}.
     */
    synchronized void add(final Socket socket) {
        if (null == socket) {
            throw new NullPointerException();
        }
        sockets[count++] = socket;
    }

    /**
     * Closes all sockets in the set.
     */
    synchronized void close() {
        for (final Socket socket : sockets) {
            try {
                if (null != socket && socket.isConnected()) {
                    socket.close();
                }
            }
            catch (final IOException e) {
            }
        }
    }

    /**
     * Indicates if this instance is complete (i.e., has all the necessary
     * sockets).
     * 
     * @return {@code true} if and only if this instance is complete.
     */
    synchronized boolean isComplete() {
        return COMPLETE_COUNT == count;
    }
}
