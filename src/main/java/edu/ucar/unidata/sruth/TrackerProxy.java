/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.NoSuchFileException;

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
     * The logger for this class.
     */
    private static Logger                 logger = Util.getLogger();
    /**
     * The address of the tracker's socket.
     */
    private final InetSocketAddress       trackerAddress;
    /**
     * The getter of the tracker's filter/server map.
     */
    private final NetworkGetter           networkGetter;
    /**
     * Whether or not this instance is closed.
     */
    @GuardedBy("this")
    private boolean                       isClosed;
    /**
     * The filter/server map specific to this instance's filter.
     */
    @GuardedBy("this")
    private FilterServerMap               filterServerMap;
    /**
     * The filter/server map for all filters.
     */
    private FilterServerMap               rawFilterServerMap;
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
     * Constructs from the address of the tracker, the data-filter to use, and
     * the address of the local server.
     * 
     * @param trackerAddress
     *            The address of the tracker.
     * @param filter
     *            The data-filter to use.
     * @param localServer
     *            The address of the local server.
     * @param distributedTrackerFiles
     *            Manager for tracker-specific administrative files.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code trackerAddress == null}.
     * @throws NullPointerException
     *             if {@code filter == null}.
     * @throws NullPointerException
     *             if {@code localServer == null}.
     * @throws NullPointerException
     *             if {@code distributedTrackerFiles == null}.
     */
    TrackerProxy(final InetSocketAddress trackerAddress, final Filter filter,
            final InetSocketAddress localServer,
            final DistributedTrackerFiles distributedTrackerFiles)
            throws IOException {
        if (null == trackerAddress) {
            throw new NullPointerException();
        }
        if (null == filter) {
            throw new NullPointerException();
        }
        if (null == distributedTrackerFiles) {
            throw new NullPointerException();
        }
        this.trackerAddress = trackerAddress;
        this.distributedTrackerFiles = distributedTrackerFiles;
        networkGetter = new NetworkGetter(filter, localServer);
        datagramSocket = new DatagramSocket();
        datagramSocket.connect(trackerAddress);
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
     * Returns the current state of the network -- specific to this instance's
     * filter -- and registers with the tracker. The actual state is returned --
     * not a copy.
     * <p>
     * This method is uninterruptible and potentially slow.
     * 
     * @return The current state of the network.
     * @throws NoSuchFileException
     *             if the tracker couldn't be contacted and there's no
     *             tracker-specific topology file in the archive.
     * @throws IllegalStateException
     *             if {@link #close()} has been called.
     * @throws IOException
     *             if an I/O error occurs.
     */
    synchronized FilterServerMap getNetwork() throws IOException {
        if (isClosed) {
            throw new IllegalStateException("Closed: " + this);
        }
        if (filterServerMap == null) {
            if (!setTopologyFromTracker()) {
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
        return filterServerMap;
    }

    /**
     * Tries to set the tracker-specific network topology information by
     * contacting the tracker.
     * 
     * @return {@code true} if and only if the attempt was successful.
     */
    private synchronized boolean setTopologyFromTracker() {
        try {
            final Socket socket = new Socket();
            socket.connect(trackerAddress, Connection.SO_TIMEOUT);
            try {
                filterServerMap = networkGetter.getNetworkAndRegister(socket);
                return true;
            }
            finally {
                try {
                    socket.close();
                }
                catch (final IOException ignored) {
                }
            }
        }
        catch (final Exception e) {
            logger.warn("Couldn't set network topology from tracker: {}: {}",
                    trackerAddress, e);
            return false;
        }
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
        final FilterServerMap currFilterServerMap = distributedTrackerFiles
                .getTopology();
        if (currFilterServerMap != rawFilterServerMap) {
            // new filter/server map
            final Filter filter = networkGetter.getFilter();
            filterServerMap = currFilterServerMap.subset(filter);
            this.rawFilterServerMap = currFilterServerMap;
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
    void reportOffline(final InetSocketAddress serverAddress)
            throws IOException {
        logger.debug("Reporting offline server {} to {}", serverAddress,
                trackerAddress);
        final byte[] buf = Util.serialize(serverAddress);
        packet.setData(buf);
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
