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
final class TopologyGetter extends TrackerTask {
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
    TopologyGetter(final Filter filter, final InetSocketAddress localServer,
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
     * Executes an instance. Creates an instance, sends it to the tracker, gets
     * the response, and adjusts the tracker proxy.
     * <p>
     * This method is potentially uninterruptible and slow.
     * 
     * @param filter
     *            Specification of locally-desired data
     * @param localServer
     *            Internet socket address of the local server
     * @param socket
     *            Socket to the tracker
     * @param trackerProxy
     *            Local proxy for the tracker
     * @throws InvalidMessageException
     *             if the response from the tracker is invalid
     * @throws SocketException
     *             if the socket is closed
     * @throws IOException
     *             if an I/O error occurs
     */
    static void execute(final Filter filter,
            final InetSocketAddress localServer, final Socket socket,
            final TrackerProxy trackerProxy) throws InvalidMessageException,
            IOException {
        final TopologyGetter topologyGetter = new TopologyGetter(filter,
                localServer, socket);
        topologyGetter.getTopologyAndRegister(trackerProxy);
    }

    /**
     * Gets the state of the network from a tracker and registers with the
     * tracker.
     * <p>
     * This method is potentially uninterruptible and slow.
     * 
     * @param trackerProxy
     *            The proxy for the tracker
     * @throws InvalidMessageException
     *             if the tracker's response is invalid.
     * @throws SocketException
     *             if the socket is closed
     * @throws IOException
     *             if an I/O error occurs.
     */
    private void getTopologyAndRegister(final TrackerProxy trackerProxy)
            throws InvalidMessageException, IOException {
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
     * @throws InvalidMessageException
     *             if the response from the tracker is invalid
     * @throws IOException
     *             if an I/O error occurs
     */
    private void processResponse(final TrackerProxy trackerProxy)
            throws InvalidMessageException, IOException {
        final ObjectInputStream ois = new ObjectInputStream(
                trackerSocket.getInputStream());
        Topology topology;
        try {
            topology = (Topology) ois.readObject();
            trackerProxy.setRawTopology(topology);
            InetSocketAddress reportingAddress;
            reportingAddress = (InetSocketAddress) ois.readObject();
            trackerProxy.setReportingAddress(reportingAddress);
        }
        catch (final ClassNotFoundException e) {
            throw new InvalidMessageException("Couldn't get topology: " + this,
                    e);
        }
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
        final Topology network = tracker.getNetwork();
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
        return "FilteredProxy [filter=" + filter + ",localServer="
                + localServer + "]";
    }

    private Object readResolve() throws ObjectStreamException, SocketException {
        try {
            return new TopologyGetter(filter, localServer, null);
        }
        catch (final NullPointerException e) {
            throw (InvalidObjectException) new InvalidObjectException(
                    this.toString()).initCause(e);
        }
    }
}
