package edu.ucar.unidata.dynaccn;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * Handles connections by clients. Starts its own thread.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class Server implements Callable<Void> {
    /**
     * The number of backlog connection requests on each port.
     */
    private static int                         BACKLOG       = 50;
    /**
     * The starting port number.
     */
    static final int                           START_PORT    = 3880;
    /**
     * The set of client-specific connections.
     */
    private final Map<InetAddress, Connection> connections   = new HashMap<InetAddress, Connection>(
                                                                     BACKLOG);
    /**
     * This instance's server-side sockets.
     */
    final ServerSocket[]                       serverSockets = new ServerSocket[Connection.SOCKET_COUNT];
    /**
     * Pathname of the root of the file hiearchy.
     */
    private final File                         dir;
    /**
     * The predicate for selecting locally-desired data.
     */
    private final Predicate                    predicate;

    /**
     * Constructs from nothing. The resulting instance will listen on all
     * available interfaces.
     * 
     * @param dir
     *            Pathname of the root of the file hierarchy.
     * @param predicate
     *            Predicate for selecting locally-desired data.
     * 
     * @throws IOException
     *             if an I/O error occurs while creating the server sockets.
     * @throws NullPointerException
     *             if {@code dir == null || predicate == null}.
     */
    Server(final String dir, final Predicate predicate) throws IOException {
        if (null == predicate) {
            throw new NullPointerException();
        }

        this.dir = new File(dir);
        this.predicate = predicate;

        for (int i = 0; i < serverSockets.length; ++i) {
            final int port = Server.START_PORT + i;

            try {
                final ServerSocket socket = new ServerSocket();

                socket.setReuseAddress(true);
                socket.bind(new InetSocketAddress(port), BACKLOG);

                serverSockets[i] = socket;
            }
            catch (final IOException e) {
                while (--i >= 0) {
                    serverSockets[i].close();
                }
                throw (IOException) new IOException("Couldn't listen on port "
                        + port).initCause(e);
            }
        }
    }

    @Override
    public Void call() throws Exception {
        try {
            for (;;) {
                accept();
            }
        }
        finally {
            for (final ServerSocket socket : serverSockets) {
                try {
                    socket.close();
                }
                catch (final IOException e) {
                    // ignored
                }
            }
        }
    }

    /**
     * Accepts a connection from a client. NOTE: Connections on the individual
     * sockets are accepted, in sequence, from lowest port to highest.
     * 
     * @throws ClassNotFoundException
     *             If the {@link RequestReceiver} receives an invalid object.
     * @throws ExecutionException
     *             if servicing the connection is terminated due to an exception
     *             thrown by one of the server's threads.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             If an I/O error occurs in the {@link RequestReceiver}.
     */
    private void accept() throws IOException, ClassNotFoundException,
            InterruptedException, ExecutionException {
        for (int i = 0; i < serverSockets.length; ++i) {
            add(serverSockets[i].accept());
        }
    }

    /**
     * Adds a socket to the collection of sockets associated with the client.
     * 
     * @param socket
     *            The socket to be added.
     * @throws ClassNotFoundException
     *             If the {@link RequestReceiver} receives an invalid object.
     * @throws ExecutionException
     *             if servicing the connection is terminated due to an exception
     *             thrown by one of the server's threads.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             If an I/O error occurs in the {@link RequestReceiver}.
     */
    private synchronized void add(final Socket socket) throws IOException,
            ClassNotFoundException, InterruptedException, ExecutionException {
        assert null != socket;
        assert socket.isConnected();

        final InetAddress inetAddress = socket.getInetAddress();
        Connection connection = connections.get(inetAddress);

        if (null == connection) {
            connection = new Connection();
            connections.put(inetAddress, connection);
        }

        connection.add(socket);

        if (connection.isComplete()) {
            connections.remove(inetAddress);
            service(connection);
        }
    }

    /**
     * Services a connection. Starts threads to service the connection.
     * 
     * @param connection
     *            The connection to be serviced.
     * @throws ClassNotFoundException
     *             If the {@link RequestReceiver} receives an invalid object.
     * @throws ExecutionException
     *             if servicing the connection is terminated due to an exception
     *             thrown by one of the server's threads.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             If an I/O error occurs in the {@link RequestReceiver}.
     */
    private Void service(final Connection connection) throws IOException,
            ClassNotFoundException, InterruptedException, ExecutionException {
        try {
            return new Peer(connection, dir, predicate).call();
        }
        finally {
            connection.close();
        }
    }
}