/**
 * Copyright 2011 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.net.SocketException;

import org.slf4j.Logger;

/**
 * A task that uses a tracker to accomplish itself.
 * 
 * @author Steven R. Emmerson
 */
abstract class TrackerTask implements Serializable {
    /**
     * The serial version identifier.
     */
    private static final long        serialVersionUID = 1L;
    /**
     * The package logger
     */
    protected static final Logger    logger           = Util.getLogger();
    /**
     * The socket to the tracker
     */
    protected final transient Socket trackerSocket;

    /**
     * Constructs from a socket that's connected to the tracker.
     * 
     * @param trackerSocket
     *            The socket to the tracker or {@code null}
     * @throws SocketException
     *             if the socket can't be configured properly
     */
    protected TrackerTask(final Socket trackerSocket) throws SocketException {
        if (trackerSocket != null) {
            trackerSocket.setSoTimeout(Connection.SO_TIMEOUT);
            trackerSocket.setKeepAlive(true);
            trackerSocket.setSoLinger(false, 0); // because flush() always
                                                 // called
            trackerSocket.setTcpNoDelay(false); // because flush() called when
                                                // appropriate
        }
        this.trackerSocket = trackerSocket;
    }

    /**
     * Sends itself to a tracker and returns the reply.
     * <p>
     * This method is uninterruptible and potentially slow.
     * 
     * @throws IOException
     *             if an I/O error occurs.
     */
    protected final void callTracker() throws IOException {
        final OutputStream outputStream = trackerSocket.getOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(outputStream);
        oos.writeObject(this);
        oos.flush();
    }

    /**
     * Interacts with a {@link Tracker} to accomplish its mission. Called by the
     * tracker.
     * 
     * @param tracker
     *            The {@link Tracker} to use.
     * @param socket
     *            The connection to the originating node if a reply is expected.
     * @throws IOException
     *             if an I/O error occurs.
     */
    abstract void process(final Tracker tracker, final Socket socket)
            throws IOException;

    /**
     * Closes the socket to the tracker. Idempotent.
     */
    protected void close() {
        try {
            trackerSocket.close();
        }
        catch (final IOException e) {
            logger.error("Couldn't close socket to tracker: {}", e.toString());
        }
    }
}
