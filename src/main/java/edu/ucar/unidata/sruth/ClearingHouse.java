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
 * 
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
        public int compare(final Peer o1, final Peer o2) {
            int cmp;
            if (o1 == o2) {
                cmp = 0;
            }
            else {
                cmp = o1.getConnection().compareTo(o2.getConnection());
                if (cmp == 0) {
                    cmp = o1.getLocalFilter().compareTo(o2.getLocalFilter());
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
     * added. Peers are considered equal if their connections are equal and
     * their data-filters for locally-desired data are equal.
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
     * @throws IOException
     *             if an I/O error occurs.
     */
    void process(final Peer peer, final PieceSpec pieceSpec) throws IOException {
        if (predicate.matches(pieceSpec) && !archive.exists(pieceSpec)) {
            peer.addRequest(pieceSpec);
        }
    }

    /**
     * Return value of method {@link #process(Peer, Piece)}.
     * 
     * @author Steven R. Emmerson
     */
    static enum PieceProcessStatus {
        NOT_USED_NOT_DONE(false, false), USED_NOT_DONE(true, false), NOT_USED_DONE(
                false, true), USED_DONE(true, true);

        private final boolean wasUsed;
        private final boolean isDone;

        PieceProcessStatus(final boolean wasUsed, final boolean isDone) {
            this.wasUsed = wasUsed;
            this.isDone = isDone;
        }

        /**
         * @return whether or not the piece of data was used.
         */
        boolean wasUsed() {
            return wasUsed;
        }

        /**
         * @return whether or not all the desired data has been received.
         */
        boolean isDone() {
            return isDone;
        }
    }

    /**
     * Processes a piece of data.
     * 
     * @param peer
     *            The local peer that received the data.
     * @param piece
     *            The piece of data to be disposed of.
     * @return a {@link PieceProcessStatus} indicating the result of the
     *         processing.
     * @throws IllegalStateException
     *             if {@code peer} is unknown.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if an I/O error occurs.
     * @see PieceProcessStatus
     */
    PieceProcessStatus process(final Peer peer, final Piece piece)
            throws IOException, InterruptedException {
        PieceProcessStatus status;
        if (!predicate.matches(piece.getFileInfo())) {
            status = predicate.matchesNothing()
                    ? PieceProcessStatus.NOT_USED_DONE
                    : PieceProcessStatus.NOT_USED_NOT_DONE;
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
            status = predicate.matchesNothing()
                    ? PieceProcessStatus.USED_DONE
                    : PieceProcessStatus.USED_NOT_DONE;
        }
        return status;
    }

    /**
     * Returns a piece of data.
     * 
     * @param pieceSpec
     *            Information on the piece of data.
     * @return The piece of data or {@code null} if a newer version of the file
     *         exists.
     * @throws IOException
     *             if an I/O error occurred.
     */
    Piece getPiece(final PieceSpec pieceSpec) throws IOException {
        return archive.getPiece(pieceSpec);
    }

    /**
     * Walks the files in the data archive.
     * <p>
     * This is a potentially lengthy operation.
     * 
     * @param consumer
     *            The consumer of data specifications.
     * @param filter
     *            The selection criteria.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void walkArchive(final FilePieceSpecSetConsumer consumer,
            final Filter filter) throws IOException {
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
     * Removes a file.
     * 
     * @param fileId
     *            Identifier of the file to be removed.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void remove(final FileId fileId) throws IOException {
        try {
            archive.remove(fileId);
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
                + peers.size() + ")]";
    }
}
