/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import net.jcip.annotations.ThreadSafe;

/**
 * Gets the current state of the network from a tracker and registers a
 * filter-specific node.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class NetworkGetter extends TrackerTask {
    /**
     * The serial version identifier.
     */
    private static final long       serialVersionUID = 1L;
    /**
     * The address of the server of the node that wants data.
     * 
     * @serial
     */
    private final InetSocketAddress localServer;
    /**
     * Specification of data desired by the client
     * 
     * @serial
     */
    private final Filter            filter;

    /**
     * Constructs from the data-filter to use and the address of the local
     * server.
     * 
     * @param filter
     *            Specification of locally-desired data
     * @param localServer
     *            The address of the local server.
     * @param trackerSocket
     *            The socket that's connected to the tracker
     * @throws SocketException
     *             if the socket can't be configured correctly
     * @throws NullPointerException
     *             if {@code filter == null}.
     * @throws NullPointerException
     *             if {@code localServer == null}.
     * @throws NullPointerException
     *             if {@code socket == null}.
     */
    NetworkGetter(final Filter filter, final InetSocketAddress localServer,
            final Socket socket) throws SocketException {
        super(socket);
        if (filter == null) {
            throw new NullPointerException();
        }
        if (localServer == null) {
            throw new NullPointerException();
        }
        this.filter = filter;
        this.localServer = localServer;
    }

    /**
     * Executes an instance. Creates an instance and has it do its thing.
     * 
     * @param filter
     *            Specification of locally-desired data
     * @param localServer
     *            Internet socket address of the local server
     * @param socket
     *            Socket to the tracker
     * @param trackerProxy
     *            Local proxy for the tracker
     * @throws IOException
     *             if an I/O error occurs
     * @throws ClassNotFoundException
     *             if the response from the tracker is invalid
     */
    static void execute(final Filter filter,
            final InetSocketAddress localServer, final Socket socket,
            final TrackerProxy trackerProxy) throws ClassNotFoundException,
            IOException {
        final NetworkGetter networkGetter = new NetworkGetter(filter,
                localServer, socket);
        networkGetter.getNetworkAndRegister(trackerProxy);
    }

    /**
     * Gets the state of the network from a tracker and registers with the
     * tracker.
     * <p>
     * This method is uninterruptible and potentially slow.
     * 
     * @param trackerProxy
     *            The proxy for the tracker
     * @throws ClassCastException
     *             if the tracker returns the wrong type.
     * @throws ClassNotFoundException
     *             if the tracker's response is invalid.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private void getNetworkAndRegister(final TrackerProxy trackerProxy)
            throws ClassNotFoundException, IOException {
        try {
            callTracker();
            processResponse(trackerProxy);
        }
        finally {
            close();
        }
    }

    /**
     * Processes the response from the tracker.
     * 
     * @param trackerProxy
     *            The proxy for the tracker
     */
    private void processResponse(final TrackerProxy trackerProxy)
            throws ClassNotFoundException, IOException {
        final ObjectInputStream ois = new ObjectInputStream(
                trackerSocket.getInputStream());
        final FilterServerMap topology = (FilterServerMap) ois.readObject();
        trackerProxy.setRawTopology(topology);
        final InetSocketAddress reportingAddress = (InetSocketAddress) ois
                .readObject();
        trackerProxy.setReportingAddress(reportingAddress);
    }

    /**
     * Replies to the client. This method is executed by the tracker.
     * <p>
     * This method is uninterruptible and potentially slow.
     * 
     * @param tracker
     *            The tracker
     * @param socket
     *            The socket on which to reply.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code socket == null}.
     */
    @Override
    public void process(final Tracker tracker, final Socket socket)
            throws IOException {
        final OutputStream outputStream = socket.getOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(outputStream);
        final FilterServerMap network = tracker.getNetwork();
        oos.writeObject(network);
        oos.writeObject(tracker.getReportingAddress());
        oos.flush();
        tracker.register(localServer, filter);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "NetworkGetter [filter=" + filter + ",localServer="
                + localServer + "]";
    }

    private Object readResolve() throws ObjectStreamException, SocketException {
        try {
            return new NetworkGetter(filter, localServer, null);
        }
        catch (final NullPointerException e) {
            throw (InvalidObjectException) new InvalidObjectException(
                    this.toString()).initCause(e);
        }
    }
}
