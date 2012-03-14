/**
 * Copyright 2011 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.Socket;

/**
 * A task that uses a tracker to accomplish itself.
 * 
 * @author Steven R. Emmerson
 */
abstract class TrackerTask implements Serializable {
    /**
     * The serial version identifier.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Constructs from nothing.
     */
    protected TrackerTask() {
    }

    /**
     * Sends itself to a tracker and returns the reply.
     * <p>
     * This method is uninterruptible and potentially slow.
     * 
     * @param socket
     *            The client socket to the tracker.
     * @return The response.
     * @throws ClassNotFoundException
     *             if the response from the server is invalid.
     * @throws IOException
     *             if an I/O error occurs.
     */
    protected final Object callTracker(final Socket socket)
            throws ClassNotFoundException, IOException {
        socket.setSoTimeout(Connection.SO_TIMEOUT);
        socket.setKeepAlive(true);
        socket.setSoLinger(false, 0); // because flush() always called
        socket.setTcpNoDelay(false); // because flush() called when appropriate
        socket.setKeepAlive(true);

        final OutputStream outputStream = socket.getOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(outputStream);
        try {
            oos.writeObject(this);
            oos.flush();
            final InputStream inputStream = socket.getInputStream();
            final ObjectInputStream ois = new ObjectInputStream(inputStream);
            try {
                return ois.readObject();
            }
            catch (final ClassNotFoundException e) {
                throw (ClassNotFoundException) new ClassNotFoundException(
                        toString()).initCause(e);
            }
            finally {
                try {
                    ois.close();
                }
                catch (final IOException ignored) {
                }
            }
        }
        finally {
            try {
                oos.close();
            }
            catch (final IOException ignored) {
            }
        }
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
     * Replies to the client.
     * <p>
     * This method is potentially slow.
     * 
     * @param socket
     *            The socket on which to reply.
     * @param reply
     *            The reply.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code socket == null}.
     */
    protected final void reply(final Socket socket, final Serializable reply)
            throws IOException {
        final OutputStream outputStream = socket.getOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(outputStream);
        try {
            oos.writeObject(reply);
        }
        finally {
            try {
                oos.close();
            }
            catch (final IOException ignored) {
            }
        }
    }
}
