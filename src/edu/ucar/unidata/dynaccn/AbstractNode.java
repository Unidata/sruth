package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;

import net.jcip.annotations.ThreadSafe;

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
     * The local server.
     */
    protected final Server        localServer;

    /**
     * Constructs from the data archive, a specification of the locally-desired
     * data, and the port numbers for the local server.
     * 
     * @param archive
     *            The data archive.
     * @param predicate
     *            Specification of the locally-desired data.
     * @param serverPorts
     *            Port numbers for the local server or {@code null}. A port
     *            number of zero will cause the operating-system to assign an
     *            ephemeral port. If {@code null} then all ports will be
     *            ephemeral.
     * @throws IOException
     *             if the server can't connect to the network.
     * @throws NullPointerException
     *             if {@code rootDir == null || predicate == null}.
     */
    AbstractNode(final Archive archive, final Predicate predicate,
            final int[] serverPorts) throws IOException {
        clearingHouse = new ClearingHouse(archive, predicate);
        localServer = new Server(clearingHouse, serverPorts);
        this.archive = archive;
    }

    /**
     * Constructs from the data archive, a specification of the locally-desired
     * data, and a range of port numbers for the local server.
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
     *             if a local server-side socket couldn't be created.
     */
    AbstractNode(final Archive archive, final Predicate predicate,
            final PortNumberSet portSet) throws IOException {
        clearingHouse = new ClearingHouse(archive, predicate);
        localServer = new Server(clearingHouse, portSet);
        this.archive = archive;
    }

    /**
     * Returns information on the local server.
     * 
     * @return Information on the local server.
     * @throws UnknownHostException
     *             if the IP address of the local host can't be obtained.
     */
    protected ServerInfo getServerInfo() throws UnknownHostException {
        return localServer.getServerInfo();
    }

    /**
     * Returns this instance's data-selection predicate.
     * 
     * @return The data-selection predicate of this instance.
     */
    Predicate getPredicate() {
        return clearingHouse.getPredicate();
    }

    /**
     * Adds a listener for client disconnections from the local server.
     * 
     * @param listener
     *            The listener for client disconnection events.
     * @throws NullPointerException
     *             if {@code listener == null}.
     */
    void addDisconnectListener(final DisconnectListener listener) {
        localServer.addDisconnectListener(listener);
    }
}