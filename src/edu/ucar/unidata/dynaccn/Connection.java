package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * A connection to a remote peer.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
abstract class Connection {
    /**
     * The number of sockets that constitute a connection.
     */
    static final int         SOCKET_COUNT = 3;
    /**
     * The indexes of the various sockets.
     */
    private static final int NOTICE       = 0;
    private static final int REQUEST      = 1;
    private static final int DATA         = 2;
    /**
     * The sockets that comprise the connection.
     */
    private final Socket[]   sockets      = new Socket[SOCKET_COUNT];
    /**
     * The current number of sockets.
     */
    private int              count        = 0;

    /**
     * Adds a socket to the set.
     * 
     * @param socket
     *            The socket to be added.
     * @return {@code true} if and only if this instance is complete (i.e., has
     *         all the necessary sockets).
     * @throws IOException
     *             if an I/O error occurs.
     * @throws IndexOutOfBoundsException
     *             if the set of sockets is already complete.
     * @throws NullPointerException
     *             if {@code socket} is {@code null}.
     */
    final synchronized boolean add(final Socket socket) throws IOException {
        if (null == socket) {
            throw new NullPointerException();
        }

        sockets[count++] = socket;

        for (int i = count - 1; 0 < i; --i) {
            if (getServerPort(sockets[i]) < getServerPort(sockets[i - 1])) {
                final Socket sock = sockets[i];

                sockets[i] = sockets[i - 1];
                sockets[i - 1] = sock;
            }
        }

        return SOCKET_COUNT == count;
    }

    /**
     * Returns the server port number of a socket.
     * 
     * @param socket
     *            The socket
     * @return The port number of the socket on the server.
     */
    protected abstract int getServerPort(Socket socket);

    /**
     * Closes all sockets in the set.
     */
    synchronized void close() {
        for (final Socket socket : sockets) {
            try {
                if (null != socket && !socket.isClosed()) {
                    socket.close();
                }
            }
            catch (final IOException e) {
            }
        }
    }

    /**
     * Returns the notice input stream.
     * 
     * @return The notice input stream.
     * @throws IOException
     *             if an I/O error occurs.
     */
    synchronized final InputStream getNoticeInputStream() throws IOException {
        return sockets[NOTICE].getInputStream();
    }

    /**
     * Returns the request input stream.
     * 
     * @return The request input stream.
     * @throws IOException
     *             if an I/O error occurs.
     */
    synchronized final InputStream getRequestInputStream() throws IOException {
        return sockets[REQUEST].getInputStream();
    }

    /**
     * Returns the data input stream.
     * 
     * @return The data input stream.
     * @throws IOException
     *             if an I/O error occurs.
     */
    synchronized final InputStream getDataInputStream() throws IOException {
        return sockets[DATA].getInputStream();
    }

    /**
     * Returns the notice output stream.
     * 
     * @return The notice output stream.
     * @throws IOException
     *             if an I/O error occurs.
     */
    synchronized final OutputStream getNoticeOutputStream() throws IOException {
        return sockets[NOTICE].getOutputStream();
    }

    /**
     * Returns the request output stream.
     * 
     * @return The request output stream.
     * @throws IOException
     *             if an I/O error occurs.
     */
    synchronized final OutputStream getRequestOutputStream() throws IOException {
        return sockets[REQUEST].getOutputStream();
    }

    /**
     * Returns the data output stream.
     * 
     * @return The data output stream.
     * @throws IOException
     *             if an I/O error occurs.
     */
    synchronized final OutputStream getDataOutputStream() throws IOException {
        return sockets[DATA].getOutputStream();
    }

    @Override
    public synchronized String toString() {
        return getClass().getSimpleName() + "{" + sockets[0].getLocalPort()
                + "<=>" + sockets[0].getPort() + ", "
                + sockets[1].getLocalPort() + "<=>" + sockets[1].getPort()
                + ", " + sockets[2].getLocalPort() + "<=>"
                + sockets[2].getPort() + "}";
    }
}
