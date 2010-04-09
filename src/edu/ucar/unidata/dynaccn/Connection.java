package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * A connection to a remote entity (server or client).
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class Connection {
    /**
     * The number of sockets that constitute a connection.
     */
    static final int         SOCKET_COUNT = 3;
    /**
     * The indexes of the various sockets.
     */
    private static final int REQUEST      = 0;
    // private static final int NOTICE = 1;
    // private static final int DATA = 2;
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
     * @throws IOException
     *             if an I/O error occurs while processing the socket.
     * @throws IndexOutOfBoundsException
     *             if the set of sockets is already complete.
     * @throws NullPointerException
     *             if {@code socket} is {@code null}.
     */
    synchronized void add(final Socket socket) throws IOException {
        if (null == socket) {
            throw new NullPointerException();
        }
        sockets[count++] = socket;
    }

    /**
     * Indicates if this instance is complete (i.e., has all the necessary
     * sockets).
     * 
     * @return {@code true} if and only if this instance is complete.
     */
    synchronized boolean isComplete() {
        return SOCKET_COUNT == count;
    }

    /**
     * Sends a request to the remote entity.
     * 
     * @param request
     *            The request to be sent.
     * @throws IOException
     *             if an I/O error occurs while sending the request.
     * @throws NullPointerException
     *             if {@code request} is {@code null}.
     */
    void write(final Request request) throws IOException {
        // TODO
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
     * Returns the stream associated with incoming requests for data.
     * 
     * @return The input request stream.
     * @throws IOException
     *             if the input request stream can't be obtained.
     */
    InputStream getInputRequestStream() throws IOException {
        return sockets[REQUEST].getInputStream();
    }

    /**
     * Returns the stream associated with outgoing requests for data.
     * 
     * @return The output request stream.
     * @throws IOException
     *             if the output request stream can't be obtained.
     */
    public OutputStream getOutputRequestStream() throws IOException {
        return sockets[REQUEST].getOutputStream();
    }
}
