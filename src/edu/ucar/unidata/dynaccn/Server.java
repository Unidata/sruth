package edu.ucar.unidata.dynaccn;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
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
     * Pathname of the directory containing files to be sent.
     */
    private final File                         outDir;
    /**
     * Pathname of the directory in which to put received files.
     */
    private final File                         inDir;

    /**
     * Constructs from nothing. The resulting instance will listen on all
     * available interfaces.
     * 
     * @param outDir
     *            Pathname of the directory containing files to be sent.
     * @param inDir
     *            Pathname of the directory in which to put received files.
     * 
     * @throws IOException
     *             if an I/O error occurs while creating the server sockets.
     * @throws NullPointerException
     *             if {@code outDir == null || indir == null}.
     */
    Server(final String outDir, final String inDir) throws IOException {
        this.inDir = new File(inDir);
        this.outDir = new File(outDir);

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

    @Override
    public Void call() throws Exception {
        for (;;) {
            accept();
        }
    }

    /**
     * Accepts a connection from a client. NOTE: Connections on the individual
     * sockets are accepted, in sequence, from lowest port to highest.
     * 
     * @throws ClassNotFoundException
     *             If the {@link RequestReceiver} receives an invalid object.
     * @throws IOException
     *             If an I/O error occurs in the {@link RequestReceiver}.
     */
    private void accept() throws Exception {
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
     * @throws ClassNotFoundException
     *             If the {@link RequestReceiver} receives an invalid object.
     * @throws IOException
     *             If an I/O error occurs in the {@link RequestReceiver}.
     */
    private synchronized void add(final Socket socket) throws Exception {
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
     * @throws IOException
     *             If an I/O error occurs in the {@link RequestReceiver}.
     * @throws ExecutionException
     *             if servicing the connection is terminated due to an exception
     *             thrown by one of the server's threads.
     */
    private Void service(final Connection connection) throws IOException,
            ClassNotFoundException, InterruptedException, ExecutionException {
        return new Peer(connection, outDir, inDir).call();
    }
}