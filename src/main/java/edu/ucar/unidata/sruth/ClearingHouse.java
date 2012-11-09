/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.FileNotFoundException;
import java.io.IOError;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;

import edu.ucar.unidata.sruth.Archive.DistributedTrackerFiles;

/**
 * Clearing house for processing pieces of data and their information.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class ClearingHouse {
    /**
     * Compares {@link Peer}s.
     * 
     * @author Steven R. Emmerson
     */
    private static final class PeerComparator implements Comparator<Peer> {
        /**
         * The sole instance of this class.
         */
        static final PeerComparator INSTANCE = new PeerComparator();

        /**
         * Constructs from nothing.
         */
        private PeerComparator() {
        }

        /*
         * Two peers are considered equal if they receive the same data from the
         * same remote node.
         * 
         * (non-Javadoc)
         * 
         * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
         */
        @Override
        public int compare(final Peer p1, final Peer p2) {
            int cmp;
            if (p1 == p2) {
                cmp = 0;
            }
            else {
                cmp = p1.getConnection().compareTo(p2.getConnection());
                if (cmp == 0) {
                    cmp = p1.getLocalFilter().compareTo(p2.getLocalFilter());
                }
            }
            return cmp;
        }
    }

    /**
     * The logger for this class.
     */
    private static final Logger   logger            = Util.getLogger();
    /**
     * The data archive.
     */
    private final Archive         archive;
    /**
     * Specification of locally-desired data.
     */
    private final Predicate       predicate;
    /**
     * All the peers using this instance.
     */
    @GuardedBy("itself")
    private final SortedSet<Peer> peers             = new TreeSet<Peer>(
                                                            PeerComparator.INSTANCE);
    /**
     * The number of completely received files.
     */
    private final AtomicLong      receivedFileCount = new AtomicLong(0);
    /**
     * The set of pending data-piece requests (i.e., requests that have been
     * sent but whose referenced data-pieces have not yet arrived)
     */
    private final SpecSet         pendingRequests   = new SpecSet();

    /**
     * Constructs from the data archive and a specification of the
     * locally-desired data.
     * 
     * @param archive
     *            The data archive.
     * @param predicate
     *            Specification of locally-desired data.
     * @throws NullPointerException
     *             if {@code archive == null || predicate == null}.
     */
    ClearingHouse(final Archive archive, final Predicate predicate) {
        if (null == predicate) {
            throw new NullPointerException();
        }
        this.archive = archive;
        this.predicate = predicate;
    }

    /**
     * Returns this instance's data archive.
     * 
     * @return this instance's data archive.
     */
    Archive getArchive() {
        return archive;
    }

    /**
     * Returns the pathname of the root of the file-tree.
     * 
     * @return The pathname of the root of the file-tree.
     */
    Path getRootDir() {
        return archive.getRootDir();
    }

    /**
     * Returns the specification of locally-desired data.
     * 
     * @return The specification of locally-desired data.
     */
    Predicate getPredicate() {
        return predicate;
    }

    /**
     * Returns the manager for distributed, tracker-specific, administrative
     * files.
     * 
     * @param trackerAddress
     *            Address of the relevant tracker.
     * @return the manager for distributed, tracker-specific, administrative
     *         files.
     */
    DistributedTrackerFiles getDistributedTrackerFiles(
            final InetSocketAddress trackerAddress) {
        return archive.getDistributedTrackerFiles(trackerAddress);
    }

    /**
     * Adds a peer. Makes the peer's
     * {@link Peer#notifyRemoteIfDesired(PieceSpec)} method eligible for
     * calling. A peer will not be added if it equals one that has already been
     * added. Peers are considered equal if they receive the same data from the
     * same remote node.
     * 
     * @param peer
     *            The peer to be added.
     * @return {@code true} if and only if the peer was added.
     * @see {@link #remove(Peer)}.
     */
    boolean add(final Peer peer) {
        synchronized (peers) {
            return peers.add(peer);
        }
    }

    /**
     * Removes a peer that was added via {@link #add(Peer)}.
     * 
     * @param peer
     *            The peer to be removed.
     */
    void remove(final Peer peer) {
        synchronized (peers) {
            peers.remove(peer);
        }
    }

    /**
     * Returns the set of peers that are exchanging at least the data specified
     * by a data-filter.
     * 
     * @param filter
     *            The data-filter.
     * @return The set of peers exchanging at least the specified data.
     */
    Collection<Peer> getPeers(final Filter filter) {
        final LinkedList<Peer> relevantPeers = new LinkedList<Peer>();
        synchronized (peers) {
            for (final Peer peer : peers) {
                if (peer.getLocalFilter().includes(filter)) {
                    relevantPeers.add(peer);
                }
            }
        }
        return relevantPeers;
    }

    /**
     * Processes a notice about a piece of data that's available at a remote
     * peer.
     * 
     * @param peer
     *            The local peer that received the notice.
     * @param pieceSpec
     *            The specification of the piece of data.
     * @throws FileInfoMismatchException
     *             if the file-information of the given piece specification
     *             doesn't match that of the extant file except for the
     *             {@link FileId}.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void process(final Peer peer, final PieceSpec pieceSpec)
            throws FileInfoMismatchException, IOException {
        if (predicate.matches(pieceSpec) && !archive.exists(pieceSpec)) {
            if (pendingRequests.add(pieceSpec)) {
                peer.queueRequest(pieceSpec);
            }
        }
    }

    /**
     * Processes a piece of data that was received by a local peer. May cause
     * the resulting, complete data-product to be processed. May block queuing
     * data-product for processing.
     * 
     * @param peer
     *            The local peer that received the piece of data.
     * @param piece
     *            The piece of data that was received by the local peer.
     * @return {@code true} if and only if the given data-piece was used.
     * @throws FileInfoMismatchException
     *             if the file-information of the given piece doesn't match that
     *             of the extant file except for the {@link FileId}.
     * @throws IllegalStateException
     *             if {@code peer} is unknown.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if an I/O error occurs.
     */
    boolean process(final Peer peer, final Piece piece)
            throws FileInfoMismatchException, IOException, InterruptedException {
        boolean wasUsed;
        if (!predicate.matches(piece.getFileInfo())) {
            wasUsed = false;
        }
        else {
            try {
                if (archive.putPiece(piece)) {
                    predicate.removeIfPossible(piece.getFileInfo());
                    receivedFileCount.incrementAndGet();
                }
                final PieceSpec pieceSpec = piece.getInfo();
                synchronized (peers) {
                    for (final Peer otherPeer : peers) {
                        if (PeerComparator.INSTANCE.compare(peer, otherPeer) != 0) {
                            otherPeer.notifyRemoteIfDesired(pieceSpec);
                        }
                    }
                }
            }
            catch (final FileNotFoundException e) {
                // The file has been deleted
                logger.debug("Can't add data to removed file \"{}\"",
                        piece.getArchivePath());
            }
            wasUsed = true;
        }
        pendingRequests.remove(piece.getInfo());
        return wasUsed;
    }

    /**
     * Indicates if all data has been received.
     * 
     * @return {@code true} if and only if all data has been received.
     */
    boolean allDataReceived() {
        return predicate.matchesNothing();
    }

    /**
     * Returns a piece of data.
     * 
     * @param pieceSpec
     *            Information on the piece of data.
     * @return The piece of data or {@code null} if a newer version of the file
     *         exists.
     * @throws FileInfoMismatchException
     *             if the file-information of the archive-file is inconsistent
     *             with that of the given piece specification
     * @throws IOException
     *             if an I/O error occurred.
     */
    Piece getPiece(final PieceSpec pieceSpec) throws FileInfoMismatchException,
            IOException {
        return archive.getPiece(pieceSpec);
    }

    /**
     * Walks the files in the data archive. Returns only when all files have
     * been visited.
     * <p>
     * This is a potentially lengthy operation.
     * 
     * @param consumer
     *            The consumer of data specifications.
     * @param filter
     *            The selection criteria.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    void walkArchive(final FilePieceSpecSetConsumer consumer,
            final Filter filter) throws IOException, InterruptedException {
        archive.walkArchive(consumer, filter);
    }

    /**
     * Returns the number of received files since this instance was created.
     * 
     * @return The number of received files.
     */
    long getReceivedFileCount() {
        return receivedFileCount.get();
    }

    /**
     * Returns the current number of contributing peers.
     * 
     * @return The current number of contributing peers.
     */
    int getPeerCount() {
        synchronized (peers) {
            return peers.size();
        }
    }

    /**
     * Removes a file if it exists.
     * 
     * @param archivePath
     *            Archive-pathname of the file to be removed.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code archivePath == null}.
     */
    void remove(final ArchivePath archivePath) throws IOException {
        try {
            archive.remove(archivePath);
        }
        catch (final IOError e) {
            throw (IOException) e.getCause();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return getClass().getSimpleName() + "[archive=" + archive + ", peers=("
                + peers.size() + "), pending request=("
                + pendingRequests.size() + ")]";
    }
}
