/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;

/**
 * Tracker-specific administrative files that are distributed via the network.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class DistributedTrackerFiles {
    /**
     * Distributes tracker-specific files via the network.
     */
    private class Distributor extends Thread {
        /**
         * The last time an attempt was made to update the files in the archive.
         */
        private ArchiveTime prevUpdateTime = ArchiveTime.BEGINNING_OF_TIME;

        @Override
        public void run() {
            for (;;) {
                /*
                 * Prevent corruption by serializing access and ensuring that
                 * the time-difference between the old and new topologies is
                 * greater than or equal to the temporal resolution of the
                 * archive.
                 */
                try {
                    final FilterServerMap topology = topologyLock.take();
                    final ArchiveTime now = new ArchiveTime();
                    if (prevUpdateTime.compareTo(now) < 0) {
                        try {
                            archive.save(topologyArchivePath, topology);
                        }
                        catch (final FileAlreadyExistsException e) {
                            logger.error(
                                    "The topology file was created by another thread!",
                                    e);
                        }
                        catch (final IOException e) {
                            logger.error("Couldn't save network topology", e);
                        }
                        prevUpdateTime = new ArchiveTime(); // ensures later
                    }
                }
                catch (final InterruptedException e) {
                    logger.error("This thread shouldn't have been interrupted",
                            e);
                }
                catch (final Throwable t) {
                    logger.error("Unexpected error", t);
                }
            }
        }
    }

    /**
     * The logger.
     */
    private static final Logger               logger            = Util.getLogger();
    /**
     * The data archive.
     */
    private final Archive                     archive;
    /**
     * The archive-pathname of the network topology file.
     */
    private final ArchivePath                 topologyArchivePath;
    /**
     * The absolute pathname of the network topology file.
     */
    private final Path                        topologyAbsolutePath;
    /**
     * The object-lock for distributing the topology. NB: This is a
     * single-element, discarding queue rather than a Hoare monitor.
     */
    private final ObjectLock<FilterServerMap> topologyLock      = new ObjectLock<FilterServerMap>();
    /**
     * Distributes tracker-specific files via the network.
     */
    @GuardedBy("this")
    private final Thread                      distributor       = new Distributor();
    /**
     * The time when the network topology file in the archive was last updated
     * via the network.
     */
    @GuardedBy("this")
    private ArchiveTime                       networkUpdateTime = ArchiveTime.BEGINNING_OF_TIME;
    /**
     * The network topology obtained via the network.
     */
    @GuardedBy("this")
    private FilterServerMap                   topologyFromNetwork;

    /**
     * Constructs from the data archive and the address of the source-node's
     * server.
     * 
     * @param archive
     *            The data archive.
     * @param trackerAddress
     *            The address of the tracker.
     * @throws NullPointerException
     *             if {@code archive == null}.
     * @throws NullPointerException
     *             if {@code trackerAddress == null}.
     */
    DistributedTrackerFiles(final Archive archive,
            final InetSocketAddress trackerAddress) {
        this.archive = archive;
        final String packagePath = getClass().getPackage().getName();
        String packageName = packagePath
                .substring(packagePath.lastIndexOf('.') + 1);
        packageName = packageName.toUpperCase();
        final ArchivePath path = new ArchivePath(Paths
                .get(trackerAddress.getHostString() + ":"
                        + trackerAddress.getPort()).resolve("FilterServerMap"));
        topologyArchivePath = archive.getAdminDir().resolve(path);
        topologyAbsolutePath = archive.resolve(topologyArchivePath);
    }

    /**
     * Returns the path in the archive of the distributed file that contains
     * tracker-specific network topology information.
     * 
     * @return the identifier of the distributed file that contains
     *         tracker-specific network topology information.
     */
    ArchivePath getTopologyArchivePath() {
        return topologyArchivePath;
    }

    /**
     * Returns the time of the last update of the tracker-specific network
     * topology. {@link #getTopology()} might modify this time. This method
     * should only be called by a subscriber.
     * 
     * @return the time of the last update of the tracker-specific network
     *         topology.
     * @throws IOException
     *             if an I/O error occurs.
     */
    synchronized ArchiveTime getTopologyArchiveTime() throws IOException {
        return networkUpdateTime;
    }

    /**
     * Returns the tracker-specific network topology information obtained via
     * the network. Might modify the value returned by
     * {@link #getTopologyArchiveTime()}. The actual object is returned -- not a
     * copy. This method should only be called by a subscriber.
     * 
     * @return the tracker-specific network topology information.
     * @throws NoSuchFileException
     *             if the tracker-specific topology file doesn't exist in the
     *             archive.
     * @throws IOException
     *             if an I/O error occurs.
     */
    synchronized FilterServerMap getTopology() throws NoSuchFileException,
            IOException {
        final ArchiveTime updateTime = new ArchiveTime(topologyAbsolutePath);
        if (networkUpdateTime.compareTo(updateTime) < 0) {
            try {
                /*
                 * If the data is updated before the time, then an older version
                 * might not be updated if a newer version arrives between those
                 * two actions.
                 */
                topologyFromNetwork = (FilterServerMap) archive.restore(
                        topologyArchivePath, FilterServerMap.class);
                networkUpdateTime = updateTime;
            }
            catch (final ClassNotFoundException e) {
                throw (IOException) new IOException(
                        "Invalid filter/server map type").initCause(e);
            }
            catch (final ClassCastException e) {
                throw (IOException) new IOException(
                        "Invalid filter/server map type").initCause(e);
            }
            catch (final FileNotFoundException e) {
                throw new NoSuchFileException(e.getLocalizedMessage());
            }
        }
        return topologyFromNetwork;
    }

    /**
     * Distribute the network topology throughout the network by saving the
     * network topology object in a file that will be subsequently distributed.
     * This method should only be called by a publisher of data.
     * 
     * @param topology
     *            The network topology.
     * @throws NullPointerException
     *             if {@code topologyFromNetwork == null}.
     */
    void distribute(final FilterServerMap topology) throws IOException {
        if (topology == null) {
            throw new NullPointerException();
        }
        ensureDistributorStarted();
        topologyLock.put(topology);
    }

    /**
     * Ensures that the distributor thread is started for distributing
     * administrative files via the network.
     */
    private synchronized void ensureDistributorStarted() {
        if (!distributor.isAlive()) {
            distributor.setDaemon(true);
            distributor.start();
        }
    }

    /**
     * Returns the data-filter that matches the filter/server map.
     * 
     * @return the data-filter that matches the filter/server map.
     */
    Filter getFilterServerMapFilter() {
        return Filter.getInstance(topologyArchivePath.toString());
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "DistributedTrackerFiles [topologyLock=" + topologyLock
                + ", networkUpdateTime=" + networkUpdateTime + "]";
    }
}
