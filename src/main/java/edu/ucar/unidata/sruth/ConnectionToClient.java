/**
 * Copyright 2011 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import net.jcip.annotations.ThreadSafe;

/**
 * A high-level connection to a remote client.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class ConnectionToClient extends Connection {
    /**
     * A single low-level connection to a remote client.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private final static class ServerSideStream extends Stream {
        /**
         * The identifier of the connection to which this stream belongs.
         */
        private final ConnectionId connectionId;

        /**
         * Constructs from a socket. This is a potentially lengthy operation.
         * 
         * @param socket
         *            The underlying socket.
         * @throws IOException
         *             if an I/O error occurs.
         * @throws NullPointerException
         *             if {@code socket == null}.
         */
        ServerSideStream(final Socket socket) throws IOException {
            super(socket);
            synchronized (this) {
                clientSocketAddress = new InetSocketAddress(
                        socket.getInetAddress(), socket.getPort());
                /*
                 * NB: The connection to the client does the reverse of the
                 * connection to the server: it constructs the object input
                 * stream first, reads from it, and then constructs the object
                 * output stream.
                 */
                input = new Stream.Input(new ObjectInputStream(
                        socket.getInputStream()));
                try {
                    /*
                     * Read from the socket the object that uniquely identifies
                     * the connection to which this stream belongs.
                     */
                    connectionId = (ConnectionId) input
                            .receiveObject(SO_TIMEOUT);
                    remoteServerSocketAddress = connectionId.getServerAddress();
                    output = new Stream.Output(new ObjectOutputStream(
                            socket.getOutputStream()));
                }
                catch (final ClassNotFoundException e) {
                    input.close();
                    throw (IOException) new IOException().initCause(e);
                }
                catch (final IOException e) {
                    input.close();
                    throw e;
                }
            }
        }
    }

    /**
     * Constructs from a socket that was just accepted by a server. This is a
     * potentially lengthy operation.
     * 
     * @param nodeId
     *            The node identifier.
     * @param socket
     *            The socket that a server just accepted.
     * @throws IllegalArgumentException
     *             if {@code streamCount < 0}.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code nodeId == null}.
     * @throws NullPointerException
     *             if {@code socket == null}.
     */
    ConnectionToClient(final Socket socket) throws IOException {
        super((InetSocketAddress) socket.getLocalSocketAddress());
        final Stream stream = new ServerSideStream(socket);
        add(stream);
    }

    /**
     * Returns an object whose {@link #hashCode()} and {@link #equals(Object)}
     * methods uniquely identify this connection over all JVM-s and for all
     * time.
     * 
     * @return an identifier object.
     */
    Object getConnectionId() {
        return ((ServerSideStream) getStream(0)).connectionId;
    }

    /**
     * Adds the single {@link ServerSideStream} of another instance to this
     * instance.
     * 
     * @param that
     *            The other instance.
     * @throws IllegalArgumentException
     *             if {@code that.size() != 1}.
     * @throws IllegalArgumentException
     *             if {@code !getConnectionId().equals(that.getConnectionId())}.
     */
    void add(final ConnectionToClient that) {
        if (that.size() != 1) {
            throw new IllegalArgumentException();
        }
        if (!getConnectionId().equals(that.getConnectionId())) {
            throw new IllegalArgumentException();
        }
        add(that.getStream(0));
    }

    @Override
    public synchronized String toString() {
        final StringBuilder buf = new StringBuilder(getClass().getSimpleName());
        buf.append("[localAddress=");
        buf.append(getSocket(0).getLocalAddress());
        buf.append(",localPort=");
        buf.append(getSocket(0).getLocalPort());
        buf.append(",remoteAddress=");
        buf.append(getSocket(0).getInetAddress());
        buf.append(",remotePorts={");
        final int n = size();
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                buf.append(",");
            }
            buf.append(getSocket(i).getPort());
        }
        buf.append("}]");
        return buf.toString();
    }
}
