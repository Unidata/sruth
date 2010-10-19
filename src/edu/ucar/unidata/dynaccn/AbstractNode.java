package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;

import net.jcip.annotations.ThreadSafe;
import edu.ucar.unidata.dynaccn.Server.DisconnectListener;

/**
 * An abstract node (reified as either a source-node or a sink-node).
 * 
 * The methods of this class are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
abstract class AbstractNode implements Callable<Void> {
    /**
     * The data archive.
     */
    protected final Archive       archive;
    /**
     * The clearing house for data.
     */
    protected final ClearingHouse clearingHouse;
    /**
     * The server.
     */
    protected final Server        server;

    /**
     * Constructs from the data archive, a specification of the locally-desired
     * data, and the port numbers for the server.
     * 
     * @param archive
     *            The data archive.
     * @param predicate
     *            Specification of the locally-desired data.
     * @param serverPorts
     *            Port numbers for the server or {@code null}. A port number of
     *            zero will cause the operating-system to assign an ephemeral
     *            port. If {@code null} then all ports will be ephemeral.
     * @throws IOException
     *             if the server can't connect to the network.
     * @throws NullPointerException
     *             if {@code rootDir == null || predicate == null}.
     */
    AbstractNode(final Archive archive, final Predicate predicate,
            final int[] serverPorts) throws IOException {
        clearingHouse = new ClearingHouse(archive, predicate);
        server = new Server(clearingHouse, serverPorts);
        this.archive = archive;
    }

    /**
     * Constructs from the data archive, a specification of the locally-desired
     * data, and a range of port numbers for the server.
     * 
     * If {@code minPort == 0 && maxPort == 0} then the operating-system will
     * assign ephemeral ports.
     * 
     * @param archive
     *            The data archive.
     * @param predicate
     *            Specification of the locally-desired data.
     * @param portSet
     *            The set of candidate port numbers.
     * @throws IOException
     *             if an unused port in the given range couldn't be found.
     * @throws NullPointerException
     *             if {@code rootDir == null || predicate == null || portSet ==
     *             null}.
     * @throws SocketException
     *             if a server-side socket couldn't be created.
     */
    AbstractNode(final Archive archive, final Predicate predicate,
            final PortNumberSet portSet) throws IOException {
        clearingHouse = new ClearingHouse(archive, predicate);
        server = new Server(clearingHouse, portSet);
        this.archive = archive;
    }

    /**
     * Returns information on the server.
     * 
     * @return Information on the server.
     * @throws UnknownHostException
     *             if the IP address of the local host can't be obtained.
     */
    protected ServerInfo getServerInfo() throws UnknownHostException {
        return server.getServerInfo();
    }

    /**
     * Adds a listener for client disconnections.
     * 
     * @param listener
     *            The listener for client disconnection events.
     * @throws NullPointerException
     *             if {@code listener == null}.
     */
    void addDisconnectListener(final DisconnectListener listener) {
        server.addDisconnectListener(listener);
    }
}