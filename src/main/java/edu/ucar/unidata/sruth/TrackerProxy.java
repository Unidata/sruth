/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.net.ConnectException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.file.NoSuchFileException;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;

import edu.ucar.unidata.sruth.Archive.DistributedTrackerFiles;

/**
 * A proxy for a tracker.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class TrackerProxy {
    /**
     * Gets filter-specific information on the network topology.
     */
    class FilteredProxy {
        /**
         * The specification of the locally-desired data
         */
        private final Filter            filter;
        /**
         * The Internet socket address of the local server
         */
        private final InetSocketAddress localServer;
        /**
         * The associated client-manager
         */
        private final ClientManager     clientManager;
        /**
         * The raw network topology that was used to compute the filter-specific
         * network topology
         */
        @GuardedBy("this")
        private Topology                rawTopology;
        /**
         * The filter-specific network topology
         */
        @GuardedBy("TrackerProxy.this")
        private Topology                filteredTopology;
        /**
         * The socket for communicating with the tracker
         */
        @GuardedBy("this")
        private Socket                  socket;
        /**
         * Whether or not this instance has been de-registered
         */
        @GuardedBy("this")
        private boolean                 deregistered;

        /**
         * Constructs from the associated client-manager.
         * 
         * @param clientManager
         *            the associated client-manager
         */
        FilteredProxy(final ClientManager clientManager) {
            filter = clientManager.getFilter();
            localServer = clientManager.getLocalServerAddress();
            this.clientManager = clientManager;
            TrackerProxy.this.register(clientManager);
        }

        /**
         * Opens the socket. Idempotent.
         */
        private synchronized void openSocket() {
            if (socket == null) {
                socket = new Socket();
            }
        }

        /**
         * Closes the socket. Idempotent.
         */
        private synchronized void closeSocket() {
            if (socket != null) {
                try {
                    socket.close();
                }
                catch (final IOException ignored) {
                }
                socket = null;
            }
        }

        /**
         * Registers the associated client-manager. Causes the client-manager to
         * be registered with the tracker and also allows the tracker proxy to
         * determine if information on the network topology is being distributed
         * via the network, which will occur if any client-manager registered
         * with the tracker proxy has at least one client.
         * <p>
         * This method is potentially slow and uninterruptible.
         * 
         * @throws SocketTimeoutException
         *             if the connection couldn't be made in
         *             {@link Connection#SO_TIMEOUT} seconds
         * @throws ConnectException
         *             if the tracker can't be contacted
         * @throws SocketException
         *             if {@link #deregister()} is called by another thread
         *             while this method is executing
         * @throws IOException
         *             if an I/O error occurs
         * @throws InvalidMessageException
         *             if the response from the tracker is invalid
         * @throws IllegalStateException
         *             if {@link #deregister()} has been called
         * @see TrackerProxy#register(ClientManager)
         */
        void register() throws SocketTimeoutException, ConnectException,
                SocketException, IOException, InvalidMessageException {
            synchronized (this) {
                if (deregistered) {
                    throw new IllegalStateException();
                }
                openSocket();
            }
            try {
                socket.connect(trackerAddress, Connection.SO_TIMEOUT);
                TopologyGetter.execute(filter, localServer, socket,
                        TrackerProxy.this);
                setTopology(TrackerProxy.this.getTopology());
                TrackerProxy.this.register(clientManager);
            }
            finally {
                closeSocket();
            }
        }

        /**
         * De-registers the associated client-manager with the tracker proxy.
         * Closes this instance. Immediately stops it from executing.
         * <p>
         * Idempotent.
         * 
         * @see TrackerProxy#deregister(ClientManager)
         */
        synchronized void deregister() {
            if (!deregistered) {
                TrackerProxy.this.deregister(clientManager);
                closeSocket();
                deregistered = true;
            }
        }

        /**
         * Returns the filter-specific information on the network topology.
         * <p>
         * This method is potentially slow and uninterruptible.
         * 
         * @return the filter-specific information on the network topology
         * @throws SocketException
         *             if {@link #deregister()} is called by another thread
         *             while this method is executing
         * @throws IOException
         *             if an I/O error occurs
         */
        Topology getTopology() throws IOException {
            openSocket();
            try {
                final Topology latestTopology = TrackerProxy.this.getTopology(
                        filter, localServer, socket);
                synchronized (this) {
                    if (rawTopology != latestTopology) {
                        setTopology(latestTopology);
                    }
                    return filteredTopology;
                }
            }
            finally {
                closeSocket();
            }
        }

        /**
         * Sets the filter-specific information on the network topology.
         * 
         * @param rawTopology
         *            The raw network topology
         */
        private synchronized void setTopology(final Topology rawTopology) {
            filteredTopology = rawTopology.subset(filter);
            this.rawTopology = rawTopology;
        }
    }

    /**
     * The logger for this class.
     */
    private static Logger                 logger         = Util.getLogger();
    /**
     * The address of the tracker's socket.
     */
    private final InetSocketAddress       trackerAddress;
    /**
     * Whether or not this instance is closed.
     */
    @GuardedBy("this")
    private boolean                       isClosed;
    /**
     * The version of the raw topology
     */
    @GuardedBy("this")
    public int                            currentVersion;
    /**
     * The raw topology
     */
    @GuardedBy("this")
    private Topology                      rawTopology;
    /**
     * The datagram socket for reporting offline servers.
     */
    private final DatagramSocket          datagramSocket;
    /**
     * The datagram for reporting offline servers.
     */
    private final DatagramPacket          packet;
    /**
     * The manager of tracker-specific administrative files.
     */
    private final DistributedTrackerFiles distributedTrackerFiles;
    /**
     * The Internet address of the socket on which to report unavailable
     * servers.
     */
    private InetSocketAddress             reportingAddress;
    /**
     * The set of {@link ClientManager}s that use this instance.
     */
    @GuardedBy("this")
    private final Set<ClientManager>      clientManagers = new TreeSet<ClientManager>(
                                                                 new Comparator<ClientManager>() {
                                                                     public int compare(
                                                                             final ClientManager o1,
                                                                             final ClientManager o2) {
                                                                         return o1
                                                                                 .getFilter()
                                                                                 .compareTo(
                                                                                         o2.getFilter());
                                                                     }
                                                                 });

    /**
     * Constructs from the address of the tracker, the data-filter to use, and
     * the address of the local server.
     * 
     * @param trackerAddress
     *            The address of the tracker.
     * @param localServer
     *            The address of the local server.
     * @param distributedTrackerFiles
     *            Manager for tracker-specific administrative files.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code trackerAddress == null}.
     * @throws NullPointerException
     *             if {@code localServer == null}.
     * @throws NullPointerException
     *             if {@code distributedTrackerFiles == null}.
     */
    TrackerProxy(final InetSocketAddress trackerAddress,
            final InetSocketAddress localServer,
            final DistributedTrackerFiles distributedTrackerFiles)
            throws IOException {
        if (null == trackerAddress) {
            throw new NullPointerException();
        }
        if (null == distributedTrackerFiles) {
            throw new NullPointerException();
        }
        this.trackerAddress = trackerAddress;
        this.distributedTrackerFiles = distributedTrackerFiles;
        datagramSocket = new DatagramSocket();
        packet = new DatagramPacket(new byte[1], 1); // buffer is irrelevant
    }

    /**
     * Returns the address of the tracker's Internet socket.
     * 
     * @return the address of the tracker's Internet socket.
     */
    InetSocketAddress getAddress() {
        return trackerAddress;
    }

    /**
     * Registers a client-manager with this instance. Allows this instance to
     * determine if information on the network topology is being distributed via
     * the network, which will occur if any registered client-manager has at
     * least one client.
     * 
     * @param clientManager
     *            The client-manager to be registered
     */
    synchronized void register(final ClientManager clientManager) {
        clientManagers.add(clientManager);
    }

    /**
     * De-registers a {@link ClientManager}. Idempotent.
     * 
     * @param clientManager
     *            The client-manager to de-register
     */
    synchronized void deregister(final ClientManager clientManager) {
        clientManagers.remove(clientManager);
    }

    /**
     * Returns a filter-specific proxy for the tracker.
     * 
     * @param clientManager
     *            The client-manager that wants the filtered proxy
     * @return a filter-specific proxy for the tracker
     */
    FilteredProxy getFilteredProxy(final ClientManager clientManager) {
        return new FilteredProxy(clientManager);
    }

    /**
     * Returns the raw state of the network.
     * 
     * @return the raw state of the network or {@code null}.
     */
    synchronized Topology getTopology() {
        return rawTopology;
    }

    /**
     * Returns the raw state of the network. Communicates with the tracker if
     * necessary. The actual state is returned -- not a copy.
     * <p>
     * This method is potentially uninterruptible and slow.
     * 
     * @param filter
     *            The specification of locally-desired data. Only used during
     *            registration with the tracker.
     * @param localServer
     *            The Internet socket address of the local server
     * @param socket
     *            The socket to use to communicate with the tracker, if
     *            necessary
     * @return The current, raw state of the network.
     * @throws NoSuchFileException
     *             if the tracker couldn't be contacted and there's no
     *             tracker-specific topology file in the archive.
     * @throws IllegalStateException
     *             if {@link #close()} has been called.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private synchronized Topology getTopology(final Filter filter,
            final InetSocketAddress localServer, final Socket socket)
            throws IOException {
        if (localServer == null) {
            throw new NullPointerException();
        }
        if (isClosed) {
            throw new IllegalStateException("Closed: " + this);
        }
        if ((rawTopology == null) || !topologyIsBeingReceived()) {
            if (!setTopologyFromTracker(filter, localServer, socket)) {
                setTopologyFromFile();
                logger.warn(
                        "Using stale network topology file {}; last modified {}",
                        distributedTrackerFiles.getTopologyArchivePath(),
                        distributedTrackerFiles.getTopologyArchiveTime());
            }
        }
        else {
            try {
                setTopologyFromFile();
            }
            catch (final NoSuchFileException e) {
                logger.info("Network topology file, {}, doesn't exist",
                        distributedTrackerFiles.getTopologyArchivePath());
            }
        }
        return rawTopology;
    }

    /**
     * Indicates if information on the network topology is being received via
     * the network.
     * 
     * @return {@code true} if and only if information on the network topology
     *         is being received via the network
     */
    private synchronized boolean topologyIsBeingReceived() {
        for (final ClientManager manager : clientManagers) {
            if (manager.getClientCount() > 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Tries to set the tracker-specific network topology information by
     * contacting the tracker.
     * <p>
     * This method is potentially uninterruptible and slow.
     * 
     * @param filter
     *            The specification of locally-desired data
     * @param localServer
     *            The Internet socket address of the local server
     * @param socket
     *            The socket to use to communicate with the tracker
     * 
     * @return {@code true} if and only if the attempt was successful.
     */
    private synchronized boolean setTopologyFromTracker(final Filter filter,
            final InetSocketAddress localServer, final Socket socket) {
        try {
            socket.connect(trackerAddress, Connection.SO_TIMEOUT);
            TopologyGetter.execute(filter, localServer, socket, this);
            return true;
        }
        catch (final Exception e) {
            // logger.error("Couldn't set network topology from tracker: "
            // + trackerAddress.toString(), e);
            logger.warn("Couldn't set network topology from tracker: {}: {}",
                    trackerAddress, e);
            return false;
        }
    }

    /**
     * Sets the raw network topology property. Used by {@link FilteredProxy}.
     * 
     * @param topology
     *            The network topology or {@code null}
     */
    synchronized void setRawTopology(final Topology topology) {
        this.rawTopology = topology;
    }

    /**
     * Sets the Internet address of the socket for reporting unavailable
     * servers. Used by {@link FilteredProxy}.
     * 
     * @param reportingAddress
     *            The Internet address of the reporting socket
     * @throws NullPointerException
     *             if {@code reportingAddress == null}
     */
    synchronized void setReportingAddress(
            final InetSocketAddress reportingAddress) {
        if (reportingAddress == null) {
            throw new NullPointerException();
        }
        this.reportingAddress = reportingAddress;
    }

    /**
     * Ensures that the tracker-specific network topology information is current
     * by updating it from the external file.
     * 
     * @throws NoSuchFileException
     *             if the external file doesn't exist.
     * @throws NoSuchFileException
     *             if the tracker-specific network topology file doesn't exist
     *             in the archive.
     * @throws IOException
     *             if a severe I/O error occurs.
     */
    private synchronized void setTopologyFromFile() throws IOException {
        final Topology currFilterServerMap = distributedTrackerFiles
                .getTopology();
        if (currFilterServerMap != rawTopology) {
            // new filter/server map
            this.rawTopology = currFilterServerMap;
        }
    }

    /**
     * Reports a server as being offline.
     * 
     * @param serverAddress
     *            The address of the server.
     * @throws IOException
     *             if an I/O error occurs.
     */
    synchronized void reportOffline(final InetSocketAddress serverAddress)
            throws IOException {
        logger.debug("Reporting offline server {} to {}", serverAddress,
                reportingAddress);
        datagramSocket.connect(reportingAddress);
        final byte[] buf = Util.serialize(serverAddress);
        packet.setData(buf);
        packet.setSocketAddress(reportingAddress);
        datagramSocket.send(packet);
    }

    /**
     * Closes this instance. Idempotent.
     */
    synchronized void close() {
        isClosed = true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "TrackerProxy [trackerAddress=" + trackerAddress + "]";
    }
}
