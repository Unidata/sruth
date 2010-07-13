package edu.ucar.unidata.dynaccn;

import java.io.IOException;
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
     * The server.
     */
    protected final Server        server;

    /**
     * Constructs from the data archive and a specification of the
     * locally-desired data.
     * 
     * @param archive
     *            The data archive.
     * @param predicate
     *            Specification of the locally-desired data.
     * @throws IOException
     *             if the server can't connect to the network.
     * @throws NullPointerException
     *             if {@code rootDir == null || predicate == null}.
     */
    AbstractNode(final Archive archive, final Predicate predicate)
            throws IOException {
        clearingHouse = new ClearingHouse(archive, predicate);
        server = new Server(clearingHouse);
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
}