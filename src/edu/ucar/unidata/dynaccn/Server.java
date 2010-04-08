package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles connections by clients. Starts its own thread.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class Server {
    private static int                        BACKLOG      = 50;
    private final Map<InetAddress, SocketSet> socketSetMap = new HashMap<InetAddress, SocketSet>(
                                                                   BACKLOG);

    /**
     * Adds a socket to the collection of ready sockets. Starts serving when
     * appropriate.
     * 
     * @param socket
     *            The socket to be added.
     * @throws NullPointerException
     *             if {@code socket} is {@code null}.
     */
    private synchronized void add(final Socket socket) {
        assert null != socket;
        assert socket.isConnected();

        final InetAddress inetAddress = socket.getInetAddress();
        SocketSet socketSet = socketSetMap.get(inetAddress);

        if (null == socketSet) {
            socketSet = new SocketSet();
        }

        socketSet.add(socket);

        if (socketSet.isComplete()) {
            socketSetMap.remove(inetAddress);
        }
    }

    /**
     * Handles a connection on a single server socket.
     * 
     * Instances are thread-compatible but not thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private class Servlet implements Runnable {
        private final ServerSocket serverSocket;

        /**
         * Constructs from the server socket.
         * 
         * @param serverSocket
         *            The server socket on which to listen.
         */
        Servlet(final ServerSocket serverSocket) {
            assert null != serverSocket;
            assert serverSocket.isBound();
            assert !serverSocket.isClosed();

            this.serverSocket = serverSocket;
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    add(serverSocket.accept());
                }
                catch (final IOException e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
    }

    static final int       START_PORT      = 3880;
    private final Thread[] serveletThreads = new Thread[SocketSet.COMPLETE_COUNT];

    /**
     * Constructs from nothing. Listens on all available interfaces. Immediately
     * starts running in an internal thread.
     * 
     * @throws IOException
     *             if an I/O error occurs.
     */
    Server() throws IOException {
        for (int i = 0; i < serveletThreads.length; ++i) {
            final int port = Server.START_PORT + i;

            try {
                final ServerSocket serverSocket = new ServerSocket(port,
                        BACKLOG);
                final Servlet servlet = new Servlet(serverSocket);
                final Thread thread = new Thread(servlet);

                thread.start();
                serveletThreads[i] = thread;
            }
            catch (final IOException e) {
                while (--i >= 0) {
                    serveletThreads[i].interrupt();
                }
                throw (IOException) new IOException("Couldn't listen on port "
                        + port).initCause(e);
            }
        }
    }
}
