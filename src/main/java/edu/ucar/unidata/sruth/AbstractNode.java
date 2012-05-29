package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.concurrent.Callable;

import net.jcip.annotations.ThreadSafe;

/**
 * An abstract node (reified as either a source-node or a sink-node).
 * <p>
 * The methods of this class are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
abstract class AbstractNode implements Callable<Void> {
    /**
     * The clearing-house for data.
     */
    protected final ClearingHouse clearingHouse;
    /**
     * The local server.
     */
    protected final Server        localServer;
    /**
     * The node identifier.
     */
    protected final NodeId        nodeId;

    /**
     * Constructs from the data archive, a specification of the locally-desired
     * data, and a set of candidate Internet socket addresses for the local
     * server.
     * 
     * @param archive
     *            The data archive.
     * @param predicate
     *            Specification of the locally-desired data.
     * @param inetSockAddrSet
     *            The set of candidate Internet socket addresses for the server.
     * @throws IOException
     *             if an unused port in the given range couldn't be found.
     * @throws NullPointerException
     *             if {@code rootDir == null || predicate == null}.
     * @throws SocketException
     *             if a local server-side socket couldn't be created.
     * @see #createServer(ClearingHouse, InetSocketAddressSet)
     */
    AbstractNode(final Archive archive, final Predicate predicate,
            final InetSocketAddressSet inetSockAddrSet) throws IOException {
        clearingHouse = new ClearingHouse(archive, predicate);
        localServer = createServer(clearingHouse, inetSockAddrSet);
        nodeId = new NodeId(localServer.getInetSocketAddress());
    }

    /**
     * Creates a server for this instance.
     * 
     * @param clearingHouse
     *            The clearing-house that the server should use.
     * @param inetSockAddrSet
     *            The set of candidate Internet socket addresses for the server.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws SocketException
     *             if a server-side socket couldn't be created.
     */
    abstract Server createServer(ClearingHouse clearingHouse,
            InetSocketAddressSet inetSockAddrSet) throws SocketException,
            IOException;

    /**
     * Returns the address of the server. May be called immediately after
     * construction.
     * 
     * @return The address of the server.
     */
    protected InetSocketAddress getServerSocketAddress() {
        return localServer.getSocketAddress();
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
     * Returns this instance's data archive.
     * 
     * @return this instance's data archive.
     */
    Archive getArchive() {
        return clearingHouse.getArchive();
    }

    /**
     * Returns the number of active servelets in this instance.
     * 
     * @return the number of active servelets in this instance.
     */
    protected int getServletCount() {
        return localServer.getServletCount();
    }

    /**
     * Returns the number of active clients in this instance.
     * 
     * @return the number of active clients in this instance.
     */
    abstract int getClientCount();
}