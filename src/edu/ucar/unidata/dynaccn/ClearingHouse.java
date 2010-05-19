/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedList;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

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
    private final LinkedList<Peer> peers = new LinkedList<Peer>();

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
    public Path getRootDir() {
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
     * Adds a peer.
     * 
     * @param peer
     *            The peer to be added.
     * @throws IllegalStateException
     *             if the peer was already added.
     */
    void add(final Peer peer) {
        synchronized (peers) {
            if (!peers.add(peer)) {
                throw new IllegalStateException("Already added: " + peer);
            }
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
        if (predicate.satisfiedBy(pieceSpec.getFileInfo())
                && !archive.exists(pieceSpec)) {
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
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IllegalStateException
     *             if {@code peer} is unknown.
     */
    boolean process(final Peer peer, final Piece piece) throws IOException,
            InterruptedException {
        if (predicate.satisfiedBy(piece.getFileInfo())) {
            if (archive.putPiece(piece)) {
                predicate.removeIfPossible(piece.getFileInfo());
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
     * @param fileSpecConsumer
     *            The consumer of data specifications.
     * @param predicate
     *            The selection criteria.
     */
    void walkArchive(final FileSpecConsumer consumer, final Predicate predicate) {
        archive.walkArchive(consumer, predicate);
    }
}
