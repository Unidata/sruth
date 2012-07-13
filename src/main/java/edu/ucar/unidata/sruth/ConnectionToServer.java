/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import net.jcip.annotations.ThreadSafe;

/**
 * The client-side of a connection to a remote server.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class ConnectionToServer extends Connection {
    /**
     * A single low-level connection to a remote server.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private final class ClientSideStream extends Stream {
        /**
         * Constructs from a socket. This is a potentially lengthy operation.
         * 
         * @param socket
         *            The underlying socket.
         * @throws IOException
         *             if an I/O error occurs.
         * @throws NullPointerException
         *             if {@code socket == null}.
         * @throws NullPointerException
         *             if {@code localServerInfo == null}.
         */
        ClientSideStream(final Socket socket) throws IOException {
            super(socket);
            synchronized (this) {
                clientSocketAddress = new InetSocketAddress(
                        socket.getLocalAddress(), socket.getLocalPort());
                /*
                 * NB: The connection to the server does the reverse of the
                 * connection to the client: it constructs the object output
                 * stream first, then the object input stream.
                 */
                output = new Stream.Output(new ObjectOutputStream(
                        socket.getOutputStream()));
                try {
                    /*
                     * Write to the socket the object that uniquely identifies
                     * the connection to which this stream belongs.
                     */
                    output.send(connectionId);

                    input = new Stream.Input(new ObjectInputStream(
                            socket.getInputStream()));
                    remoteServerSocketAddress = new InetSocketAddress(
                            socket.getInetAddress(), socket.getPort());
                }
                catch (final IOException e) {
                    output.close();
                    throw e;
                }
            }
        }
    }

    /**
     * The object that uniquely identifies this connection.
     */
    private final ConnectionId  connectionId;
    /**
     * The address of the remote server.
     */
    private final SocketAddress remoteServer;
    private final Socket[]      sockets = new Socket[STREAM_COUNT];

    /**
     * Constructs from the address of the local server and the address of a
     * remote server.
     * 
     * @param localServer
     *            Address of the local server.
     * @param remoteServer
     *            Address of the remote server.
     * @throws NullPointerException
     *             if {@code localServer == null}.
     * @throws NullPointerException
     *             if {@code remoteServer == null}.
     */
    ConnectionToServer(final InetSocketAddress localServer,
            final InetSocketAddress remoteServer) {
        super(localServer);
        if (remoteServer == null) {
            throw new NullPointerException();
        }
        this.remoteServer = remoteServer;
        connectionId = new ConnectionId(localServer);
        for (int i = 0; i < STREAM_COUNT; i++) {
            sockets[i] = new Socket();
        }
    }

    /**
     * Opens the connection.
     * <p>
     * This is an uninterruptible and potentially lengthy operation. The timeout
     * is set by the user-preference {@value #SO_TIMEOUT_KEY} (default
     * {@value #SO_TIMEOUT_DEFAULT}).
     * 
     * @throws SocketTimeoutException
     *             if a connection can't be made to the remote server within the
     *             timeout
     * @throws SocketException
     *             if an error occurs on the socket
     * @throws IOException
     *             if an I/O error occurs
     */
    void open() throws SocketTimeoutException, SocketException, IOException {
        try {
            for (final Socket socket : sockets) {
                socket.setSoLinger(false, 0); // because flush() always called
                socket.setTcpNoDelay(false); // because flush() called when
                                             // appropriate
                socket.setKeepAlive(true);

                socket.connect(remoteServer, SO_TIMEOUT);
                add(new ClientSideStream(socket));
            }
        }
        catch (final IOException e) {
            close();
            if (e instanceof SocketTimeoutException) {
                throw (SocketTimeoutException) e;
            }
            if (e instanceof SocketException) {
                throw (SocketException) e;
            }
            throw e;
        }
    }

    /**
     * Closes this instance, releasing all resources. Idempotent.
     */
    @Override
    void close() {
        for (final Socket socket : sockets) {
            try {
                socket.close();
            }
            catch (final IOException ignored) {
            }
        }
        super.close();
    }

    @Override
    public synchronized String toString() {
        final StringBuilder buf = new StringBuilder(getClass().getSimpleName());
        buf.append("[localAddress=");
        buf.append(getSocket(0).getLocalAddress());
        buf.append(",localPorts={");
        final int n = size();
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                buf.append(",");
            }
            buf.append(getSocket(i).getLocalPort());
        }
        buf.append("},remoteAddress=");
        buf.append(getSocket(0).getInetAddress());
        buf.append(",remotePort=");
        buf.append(getSocket(0).getPort());
        buf.append("]");
        return buf.toString();
    }
}
