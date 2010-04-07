package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

/**
 * A connection to a remote server.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class Connection {
    private final SocketSet sockets = new SocketSet();

    /**
     * Constructs from an Internet address. The connection is immediately opened
     * and may be used.
     * 
     * @param inetAddress
     *            The Internet address.
     * @throws IOException
     *             if unable to connect to the remote server.
     * @throws NullPointerException
     *             if {@code addr} is {@code null}.
     */
    public Connection(final InetAddress inetAddress) throws IOException {
        for (int i = 0; i < SocketSet.COMPLETE_COUNT; i++) {
            final int port = Server.START_PORT + i;

            try {
                sockets.add(new Socket(inetAddress, port));
            }
            catch (final IOException e) {
                sockets.close();
                throw (IOException) new IOException("Couldn't connect to port "
                        + port + " on host " + inetAddress).initCause(e);
            }
        }
    }

    /**
     * Closes the connection to the remote server. The instance cannot be used
     * after this call.
     */
    public void close() {
        sockets.close();
    }
}
