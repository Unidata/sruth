/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.FileNotFoundException;
import java.io.IOError;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * The logger for this class.
     */
    private static final Logger    logger            = LoggerFactory
                                                             .getLogger(ClearingHouse.class);
    /**
     * The data archive.
     */
    private final Archive          archive;
    /**
     * Specification of locally-desired data.
     */
    private final Predicate        predicate;
    /**
     * All the peers using this instance.
     */
    @GuardedBy("itself")
    private final LinkedList<Peer> peers             = new LinkedList<Peer>();
    /**
     * The number of completely received files.
     */
    private final AtomicLong       receivedFileCount = new AtomicLong(0);

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
     * Adds a peer. Makes the peer's
     * {@link Peer#notifyRemoteIfDesired(PieceSpec)} method eligible for
     * calling.
     * 
     * @param peer
     *            The peer to be added.
     * @throws IllegalStateException
     *             if the peer was already added.
     * @see {@link #remove(Peer)}.
     */
    void add(final Peer peer) {
        synchronized (peers) {
            if (!peers.add(peer)) {
                throw new IllegalStateException("Already added: " + peer);
            }
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
        if (predicate.satisfiedBy(pieceSpec) && !archive.exists(pieceSpec)) {
            peer.addRequest(pieceSpec);
        }
    }

    /**
     * Processes a piece of data.
     * 
     * @param peer
     *            The local peer that received the data.
     * @param piece
     *            The piece of data to be disposed of.
     * @return {@code true} if all data has been received.
     * @throws IllegalStateException
     *             if {@code peer} is unknown.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if an I/O error occurs.
     */
    boolean process(final Peer peer, final Piece piece) throws IOException,
            InterruptedException {
        if (predicate.satisfiedBy(piece.getFileInfo())) {
            try {
                if (archive.putPiece(piece)) {
                    predicate.removeIfPossible(piece.getFileInfo());
                    receivedFileCount.incrementAndGet();
                }
                final PieceSpec pieceSpec = piece.getInfo();
                synchronized (peers) {
                    for (final Peer otherPeer : peers) {
                        if (!peer.equals(otherPeer)) {
                            otherPeer.notifyRemoteIfDesired(pieceSpec);
                        }
                    }
                }
            }
            catch (final FileNotFoundException e) {
                // The file has been deleted
                logger.debug("Can't add data to removed file \"{}\"", piece
                        .getPath());
            }
        }
        return predicate.satisfiedByNothing();
    }

    /**
     * Returns a piece of data.
     * 
     * @param pieceSpec
     *            Information on the piece of data.
     * @return The piece of data.
     * @throws IOException
     *             if an I/O error occurred.
     */
    Piece getPiece(final PieceSpec pieceSpec) throws IOException {
        return archive.getPiece(pieceSpec);
    }

    /**
     * Walks the files in the data archive.
     * 
     * @param consumer
     *            The consumer of data specifications.
     * @param predicate
     *            The selection criteria.
     */
    void walkArchive(final FilePieceSpecSetConsumer consumer,
            final Predicate predicate) {
        archive.walkArchive(consumer, predicate);
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
     * Removes a file or category.
     * 
     * @param fileId
     *            Specification of the file or category to be removed.
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
        return "ClearingHouse [archive=" + archive + "]";
    }
}
