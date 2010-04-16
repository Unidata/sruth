package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
    private static final int NOTICE       = 1;
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
     * Returns an {@link ObjectInputStream} that wraps a socket.
     * 
     * @param socket
     *            The socket to wrap.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private static ObjectInputStream wrapInput(final Socket socket)
            throws IOException {
        return new ObjectInputStream(socket.getInputStream());
    }

    /**
     * Returns an {@link ObjectOutputStream} that wraps a socket.
     * 
     * @param socket
     *            The socket to wrap.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private static ObjectOutputStream wrapOutput(final Socket socket)
            throws IOException {
        return new ObjectOutputStream(socket.getOutputStream());
    }

    /**
     * Returns the stream associated with incoming requests for data.
     * 
     * @return The input request stream.
     * @throws IOException
     *             if the input request stream can't be obtained.
     */
    ObjectInputStream getRequestInputStream() throws IOException {
        return wrapInput(sockets[REQUEST]);
    }

    /**
     * Returns the stream associated with outgoing requests for data.
     * 
     * @return The output request stream.
     * @throws IOException
     *             if the stream can't be obtained.
     */
    ObjectOutputStream getRequestOutputStream() throws IOException {
        return wrapOutput(sockets[REQUEST]);
    }

    /**
     * Returns the stream associated with incoming notices of data.
     * 
     * @return The input notice stream.
     * @throws IOException
     *             if the input notice stream can't be obtained.
     */
    ObjectInputStream getNoticeInputStream() throws IOException {
        return wrapInput(sockets[NOTICE]);
    }

    /**
     * Returns the stream associated with outgoing notices of data.
     * 
     * @return The output notice stream.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws IOException
     *             if the stream can't be obtained.
     */
    ObjectOutputStream getNoticeOutputStream() throws IOException {
        return wrapOutput(sockets[NOTICE]);
    }

    /**
     * Returns the stream associated with incoming pieces of data.
     * 
     * @return The input data-piece stream.
     * @throws IOException
     *             if the input data-piece stream can't be obtained.
     */
    ObjectInputStream getDataInputStream() throws IOException {
        return wrapInput(sockets[DATA]);
    }

    /**
     * Returns the stream associated with outgoing pieces of data.
     * 
     * @return The output data-piece stream.
     * @throws IOException
     *             if the output data-piece stream can't be obtained.
     */
    ObjectOutputStream getDataOutputStream() throws IOException {
        return wrapOutput(sockets[DATA]);
    }
}
