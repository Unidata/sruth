package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

/**
 * Handles connections by clients. Starts its own thread.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class Server {
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
     * Constructs from nothing. The resulting instance will listen on all
     * available interfaces.
     * 
     * @throws IOException
     *             if an I/O error occurs while creating the server sockets.
     */
    private Server() throws IOException {
        for (int i = 0; i < serverSockets.length; ++i) {
            final int port = Server.START_PORT + i;

            try {
                serverSockets[i] = new ServerSocket(port, BACKLOG);
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

    /**
     * Creates a server and starts executing it.
     * 
     * @return The server's {@link Future}.
     * @throws IOException
     *             if an I/O error occurs while creating the server sockets.
     */
    static Future<Void> start() throws IOException {
        final Server server = new Server();
        final FutureTask<Void> future = new FutureTask<Void>(
                new Callable<Void>() {
                    public Void call() throws IOException {
                        for (;;) {
                            server.accept();
                        }
                    }
                });
        new Thread(future).start();
        return future;
    }

    /**
     * Accepts a connection from a client. NOTE: Connections on the individual
     * sockets are accepted, in sequence, from lowest port to highest.
     * 
     * @throws IOException
     *             if an I/O error occurs.
     */
    private void accept() throws IOException {
        for (int i = 0; i < serverSockets.length; ++i) {
            try {
                add(serverSockets[i].accept());
            }
            catch (final IOException e) {
                for (int j = 0; j < serverSockets.length; ++j) {
                    try {
                        serverSockets[j].close();
                    }
                    catch (final IOException e1) {
                    }
                }
                throw e;
            }
        }
    }

    /**
     * Adds a socket to the collection of sockets associated with the client.
     * 
     * @param socket
     *            The socket to be added.
     * @throws IOException
     *             if an I/O error occurs while processing the socket.
     * @throws NullPointerException
     *             if {@code socket} is {@code null}.
     */
    private synchronized void add(final Socket socket) throws IOException {
        assert null != socket;
        assert socket.isConnected();

        final InetAddress inetAddress = socket.getInetAddress();
        Connection connection = connections.get(inetAddress);

        if (null == connection) {
            connection = new Connection();
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
     * @throws IOException
     *             if an I/O error occurs while processing the connection.
     */
    private void service(final Connection connection) throws IOException {
        RequestReceiver.start(connection.getInputRequestStream());
    }
}