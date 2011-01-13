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
     * @throws IllegalArgumentException
     *             if the remote IP address of the socket doesn't equal that of
     *             the previously-added sockets.
     * @throws IndexOutOfBoundsException
     *             if the set of sockets is already complete.
     * @throws NullPointerException
     *             if {@code socket} is {@code null}.
     */
    synchronized boolean add(final Socket socket) {
        if (null == socket) {
            throw new NullPointerException();
        }

        if (0 < count) {
            if (!socket.getInetAddress().equals(sockets[0].getInetAddress())) {
                throw new IllegalArgumentException(socket.getInetAddress()
                        .toString()
                        + " != " + sockets[0].getInetAddress().toString());
            }
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
     * Returns the localServer port number of a socket.
     * 
     * @param socket
     *            The socket
     * @return The port number of the socket on the localServer.
     */
    protected abstract int getServerPort(Socket socket);

    /**
     * Closes all sockets in the set.
     */
    synchronized void close() {
        for (final Socket socket : sockets) {
            if (null != socket && !socket.isClosed()) {
                try {
                    socket.close();
                }
                catch (final IOException ignored) {
                }
            }
        }
    }

    /**
     * Returns the REQUEST socket.
     * 
     * @return The REQUEST socket.
     */
    Socket getRequestSocket() {
        return sockets[REQUEST];
    }

    /**
     * Returns the NOTICE socket.
     * 
     * @return The NOTICE socket.
     */
    Socket getNoticeSocket() {
        return sockets[NOTICE];
    }

    /**
     * Returns the DATA socket.
     * 
     * @return The DATA socket.
     */
    Socket getDataSocket() {
        return sockets[DATA];
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

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + count;
        if (0 < count) {
            result = prime * result + sockets[0].getInetAddress().hashCode();
        }
        for (int i = 0; i < count; ++i) {
            result = prime * result + sockets[i].getLocalPort()
                    + sockets[i].getPort();
        }
        return result;
    }

    /**
     * Indicates if this instance is considered equal to an object. Two
     * instances are equal if and only if they have the same set of local and
     * remote ports and remote IP address.
     * 
     * @param obj
     *            The object for testing.
     * @return {@code true} if and only this instance is considered equal to
     *         {@code obj}.
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Connection other = (Connection) obj;
        if (count != other.count) {
            return false;
        }
        if (0 < count) {
            if (!sockets[0].getInetAddress().equals(
                    other.sockets[0].getInetAddress())) {
                return false;
            }
        }
        for (int i = 0; i < count; ++i) {
            if (sockets[i].getLocalPort() != other.sockets[i].getLocalPort()) {
                return false;
            }
            if (sockets[i].getPort() != other.sockets[i].getPort()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public synchronized String toString() {
        return getClass().getSimpleName() + "[remote="
                + sockets[0].getInetAddress() + ",localPorts=["
                + sockets[0].getLocalPort() + "," + sockets[1].getLocalPort()
                + "," + sockets[2].getLocalPort() + "]" + ",remotePorts=["
                + sockets[0].getPort() + "," + sockets[1].getPort() + ","
                + sockets[2].getPort() + "]]";
    }
}
