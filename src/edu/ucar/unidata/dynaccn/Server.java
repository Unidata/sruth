package edu.ucar.unidata.dynaccn;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Handles connections by clients. Starts its own thread.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class Server implements Callable<Void> {
    /**
     * Listens to a port for incoming connections.
     * 
     * Instances are thread-compatible but not thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private class Listener implements Callable<Void> {
        /**
         * The server socket.
         */
        private final ServerSocket serverSocket;

        /**
         * Constructs from the number of the port on which to listen.
         * 
         * @param port
         *            The port number. If less than or equal to zero, then an
         *            unused port will be chosen by the system.
         * @throws IOException
         *             if an I/O error occurs
         */
        Listener(final int port) throws IOException {
            serverSocket = new ServerSocket();
            try {
                serverSocket.setReuseAddress(true);
                serverSocket.bind(new InetSocketAddress(port));
            }
            catch (final SocketException e) {
                try {
                    serverSocket.close();
                }
                catch (final Exception e2) {
                    // ignored
                }
                throw e;
            }
            catch (final IOException e) {
                try {
                    serverSocket.close();
                }
                catch (final Exception e2) {
                    // ignored
                }
                throw e;
            }
        }

        /**
         * Doesn't return normally. Closes the server socket.
         * 
         * @throws IOException
         *             if an I/O error occurs.
         */
        @Override
        public Void call() throws IOException {
            try {
                for (;;) {
                    final Socket socket = serverSocket.accept();
                    try {
                        final InetAddress inetAddr = socket.getInetAddress();
                        Connection connection = new ServerConnection();
                        final Connection prevConnection = connections
                                .putIfAbsent(inetAddr, connection);
                        if (null != prevConnection) {
                            connection = prevConnection;
                        }
                        if (connection.add(socket)) {
                            System.out.println("Server: " + connection);
                            peerExecutor.submit(new Peer(connection, dir,
                                    Predicate.NOTHING));
                        }
                    }
                    catch (final IOException e) {
                        socket.close();
                        throw e;
                    }
                }
            }
            finally {
                try {
                    serverSocket.close();
                }
                catch (final IOException e) {
                    // ignored
                }
            }
        }

        /**
         * Closes this instance.
         * 
         * @throws IOException
         *             if an error occurs while closing the socket.
         */
        void close() throws IOException {
            serverSocket.close();
        }
    }

    /**
     * The port listeners.
     */
    private final Listener[]                             listeners        = new Listener[Connection.SOCKET_COUNT];
    /**
     * Executes peers created by this instance.
     */
    private final ExecutorService                        peerExecutor     = Executors
                                                                                  .newCachedThreadPool();
    /**
     * Executes port listeners.
     */
    private final ExecutorService                        listenerExecutor = Executors
                                                                                  .newFixedThreadPool(listeners.length);
    /**
     * Manages port listeners.
     */
    private final ExecutorCompletionService<Void>        listenerManager  = new ExecutorCompletionService<Void>(
                                                                                  listenerExecutor);
    /**
     * The starting port number.
     */
    static final int                                     START_PORT       = 3880;
    /**
     * The set of client-specific connections.
     */
    private final ConcurrentMap<InetAddress, Connection> connections      = new ConcurrentHashMap<InetAddress, Connection>();
    /**
     * Pathname of the root of the file hierarchy.
     */
    private final File                                   dir;

    /**
     * Constructs from the root of the file-tree. Immediately starts listening
     * for connection attempts but doesn't process the attempts until method
     * {@link #call()} is called.
     * 
     * By default, the resulting instance will listen on all available
     * interfaces.
     * 
     * @param dir
     *            Pathname of the root of the file hierarchy.
     * @throws IOException
     *             if a port can't be listened to.
     */
    Server(final String dir) throws IOException {
        this.dir = new File(dir);
        for (int i = 0; i < listeners.length; i++) {
            try {
                listeners[i] = new Listener(START_PORT + i);
            }
            catch (final IOException e) {
                while (--i >= 0) {
                    try {
                        listeners[i].close();
                    }
                    catch (final IOException e2) {
                        // ignored
                    }
                }
                throw e;
            }
        }
    }

    /**
     * Executes this instance and waits upon one of the following conditions: 1)
     * an error occurs; or 2) the current thread is interrupted. In any case,
     * any and all subtasks will have been terminated upon return.
     * 
     * @throws ExecutionException
     *             if a subtask terminates due to an error.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    @Override
    public Void call() throws InterruptedException, ExecutionException {
        try {
            for (final Listener listener : listeners) {
                listenerManager.submit(listener);
            }
            try {
                final Future<Void> future = listenerManager.take();

                try {
                    future.get();
                    listenerExecutor.shutdownNow();
                }
                catch (final ExecutionException e) {
                    listenerExecutor.shutdownNow();
                    throw e;
                }
                catch (final CancellationException e) {
                    // Can't happen
                }
            }
            catch (final InterruptedException e) {
                listenerExecutor.shutdownNow();
                throw e;
            }
        }
        finally {
            for (final Listener listener : listeners) {
                try {
                    listener.close();
                }
                catch (final IOException e) {
                    // ignored
                }
            }
        }
        return null;
    }
}