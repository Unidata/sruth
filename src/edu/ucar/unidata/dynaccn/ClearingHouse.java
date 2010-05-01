/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Clearing house for processing pieces of data and their information.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class ClearingHouse {
    /**
     * The root of the file-tree.
     */
    private final File                           rootDir;
    /**
     * Specification of locally-desired data.
     */
    private final Predicate                      predicate;
    /**
     * All the peers using this instance.
     */
    private final ConcurrentMap<Peer, PeerEntry> peers = new ConcurrentHashMap<Peer, PeerEntry>();

    /**
     * Constructs from the root of the file-tree and a specification of the
     * locally-desired data.
     * 
     * @param rootDir
     *            The root of the file-tree.
     * @param predicate
     *            Specification of locally-desired data.
     * @throws NullPointerException
     *             if {@code rootDir == null || predicate == null}.
     */
    ClearingHouse(final File rootDir, final Predicate predicate) {
        if (null == rootDir || null == predicate) {
            throw new NullPointerException();
        }
        this.rootDir = rootDir;
        this.predicate = predicate;
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
     * @throws IllegalArgumentException
     *             if {@code peer} was already added.
     */
    void addPeer(final Peer peer) {
        final PeerEntry peerEntry = new PeerEntry();
        if (null != peers.putIfAbsent(peer, peerEntry)) {
            throw new IllegalArgumentException("Already added: " + peer);
        }
    }

    /**
     * Returns the peer-entry corresponding to a local peer.
     * 
     * @param peer
     *            The local peer.
     * @return The corresponding peer-entry.
     * @throws IllegalStateException
     *             if {@code peer} is unknown.
     */
    private PeerEntry getPeerEntry(final Peer peer) {
        final PeerEntry peerEntry = peers.get(peer);
        if (null == peerEntry) {
            throw new IllegalStateException("Unknown peer: " + peer);
        }
        return peerEntry;
    }

    /**
     * Waits for {@link #process(Peer, PredicateRequest)} to be called.
     * 
     * @param peer
     *            The local peer that's making this call.
     * @throws IllegalStateException
     *             if {@code peer} hasn't been previously added to this
     *             instance.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    void waitForRemotePredicate(final Peer peer) throws InterruptedException {
        // {@link #getNextNotice(Peer)} needs the remote predicate
        getPeerEntry(peer).waitForRemotePredicate();
    }

    /**
     * Processes a specification of data desired by the remote peer.
     * 
     * @param peer
     *            The local peer that's making this call.
     * @param predicateRequest
     *            The specification of remotely-desired data.
     * @throws IllegalStateException
     *             if {@code peer} hasn't been previously added to this
     *             instance.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    void process(final Peer peer, final PredicateRequest predicateRequest)
            throws InterruptedException {
        getPeerEntry(peer).processRemote(predicateRequest);
    }

    /**
     * Processes a request for a piece of data.
     * 
     * @param peer
     *            The local peer that's making this call.
     * @param pieceRequest
     *            The request for a piece of data.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    void process(final Peer peer, final PieceRequest pieceRequest)
            throws InterruptedException, IOException {
        getPeerEntry(peer).processRemote(pieceRequest);
    }

    /**
     * Processes a notice of a file available at the remote peer.
     * 
     * @param fileNotice
     *            The notice of an available file.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void process(final FileNotice fileNotice) throws IOException {
        final FileInfo fileInfo = fileNotice.getFileInfo();
        if (predicate.satisfiedBy(fileInfo)) {
            DiskFile.create(rootDir, fileInfo);
        }
    }

    /**
     * Processes a notice of an available piece of data at a remote peer.
     * 
     * @param peer
     *            The local peer that's making this call.
     * @param pieceNotice
     *            Notice of the available piece of data.
     * @throws IllegalStateException
     *             if {@code peer} is unknown.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    void process(final Peer peer, final PieceNotice pieceNotice)
            throws InterruptedException {
        getPeerEntry(peer).processRemote(pieceNotice);
    }

    /**
     * Processes a piece of data.
     * 
     * @param peer
     *            The local peer that's making this call.
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
            if (DiskFile.putPiece(rootDir, piece)) {
                predicate.removeIfPossible(piece.getFileInfo());
            }
        }
        for (final Map.Entry<Peer, PeerEntry> mapEntry : peers.entrySet()) {
            final Peer otherPeer = mapEntry.getKey();
            if (!peer.equals(otherPeer)) {
                mapEntry.getValue().notifyRemoteIfDesired(piece.getInfo());
            }
        }
        return predicate.satisfiedByNothing();
    }

    /**
     * Returns the next request to make of the remote peer.
     * 
     * @param peer
     *            The local peer that's making this call.
     * @return The next request to make of the remote peer.
     * @throws IllegalStateException
     *             if {@code peer} is unknown.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    Request getNextRequest(final Peer peer) throws InterruptedException {
        return getPeerEntry(peer).getNextRequest();
    }

    /**
     * Returns the next notice that should be sent to the remote peer.
     * 
     * @param peer
     *            The local peer that's making this call.
     * @return The next notice.
     * @throws IllegalStateException
     *             if {@code peer} is unknown.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    Notice getNextNotice(final Peer peer) throws InterruptedException {
        return getPeerEntry(peer).getNextNotice();
    }

    /**
     * Returns the next piece of data to send to the remote peer.
     * 
     * @param peer
     *            The local peer that's making this call.
     * @return The next piece of data.
     * @throws IllegalStateException
     *             if {@code peer} is unknown.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    Piece getNextPiece(final Peer peer) throws InterruptedException {
        return getPeerEntry(peer).getNextPiece();
    }

    /**
     * An entry for a local peer.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private final class PeerEntry {
        /**
         * Rendezvous-queue for the remotely-desired data.
         */
        private final SynchronousQueue<Predicate> remotePredicateQueue = new SynchronousQueue<Predicate>();
        /**
         * Specification of data desired by the remote peer.
         */
        private final AtomicReference<Predicate>  remotePredicate      = new AtomicReference<Predicate>();
        /**
         * Notice queue. Contains notices of data to be sent to the remote peer.
         */
        private final BlockingQueue<Notice>       noticeQueue          = new SynchronousQueue<Notice>();
        /**
         * Request queue. Contains specifications of data-pieces to request from
         * the remote peer.
         */
        private final BlockingQueue<Request>      requestQueue         = new ArrayBlockingQueue<Request>(
                                                                               1);
        /**
         * Piece queue. Contains pieces of data to be sent to the remote peer.
         */
        private final BlockingQueue<Piece>        pieceQueue           = new SynchronousQueue<Piece>();
        /**
         * Iterator over the files to send to the remote peer.
         */
        private Iterator<Notice>                  fileIterator;

        /**
         * Constructs from nothing.
         */
        PeerEntry() {
            /*
             * Prime the request-queue with a specification of the
             * locally-desired data.
             */
            try {
                requestQueue.put(new PredicateRequest(predicate));
            }
            catch (final InterruptedException e) {
                // Can't happen
                throw new AssertionError();
            }
        }

        /**
         * Notify the remote peer about an available piece of data.
         * 
         * @param pieceInfo
         *            Information on the available piece of data.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         */
        void notifyRemoteIfDesired(final PieceInfo pieceInfo)
                throws InterruptedException {
            if (remotePredicate.get().satisfiedBy(pieceInfo.getFileInfo())) {
                noticeQueue.put(new PieceNotice(pieceInfo));
            }
        }

        /**
         * Waits for the specification of remotely-desired data. Starts a thread
         * that scans the file-tree for matching files.
         * 
         * @throws InterruptedException
         *             if the current thread is interrupted.
         */
        void waitForRemotePredicate() throws InterruptedException {
            remotePredicate.set(remotePredicateQueue.take());

            new Thread(new FutureTask<Void>(new Callable<Void>() {
                @Override
                public Void call() throws InterruptedException {
                    fileIterator = new FileIterator(rootDir, rootDir);
                    final Predicate predicate = remotePredicate.get();
                    while (fileIterator.hasNext()) {
                        final Notice notice = fileIterator.next();
                        if (predicate.satisfiedBy(notice.getFileInfo())) {
                            noticeQueue.put(notice);
                        }
                    }
                    return null;
                }
            }), "FileIterator").start();
        }

        /**
         * Saves a specification of remotely-desired data.
         * 
         * @param predicateRequest
         *            Request for remotely-desired data.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         */
        void processRemote(final PredicateRequest predicateRequest)
                throws InterruptedException {
            remotePredicateQueue.put(predicateRequest.getPredicate());
        }

        /**
         * Processes a request for a piece of data.
         * 
         * @param peer
         *            The local peer that's making this call.
         * @param pieceRequest
         *            The request for a piece of data.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         * @throws IOException
         *             if an I/O error occurs.
         */
        void processRemote(final PieceRequest pieceRequest)
                throws InterruptedException, IOException {
            pieceQueue.put(DiskFile.getPiece(rootDir, pieceRequest
                    .getPieceInfo()));
        }

        /**
         * Processes a notice of an available piece of data at the remote peer.
         * 
         * @param pieceNotice
         *            Notice of the available piece of data.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         */
        void processRemote(final PieceNotice pieceNotice)
                throws InterruptedException {
            final PieceInfo pieceInfo = pieceNotice.getPieceInfo();
            if (predicate.satisfiedBy(pieceInfo)) {
                requestQueue.put(new PieceRequest(pieceInfo));
            }
        }

        /**
         * Returns the next request to make of the remote peer.
         * 
         * @return The next request to make of the remote peer.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         */
        Request getNextRequest() throws InterruptedException {
            return requestQueue.take();
        }

        /**
         * Returns the next notice that should be sent to the remote peer.
         * 
         * @return The next notice.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         */
        Notice getNextNotice() throws InterruptedException {
            return noticeQueue.take();
        }

        /**
         * Returns the next piece of data to send to the remote peer.
         * 
         * @param peer
         *            The local peer that's making this call.
         * @return The next piece of data.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         */
        Piece getNextPiece() throws InterruptedException {
            return pieceQueue.take();
        }
    }

    /**
     * Iterates over the pieces of regular files in a directory. Doesn't
     * recurse.
     * 
     * Instances are thread-compatible but not thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private static class FileIterator implements Iterator<Notice> {
        /**
         * The canonical size, in bytes, of a piece of data (131072).
         */
        private final int           PIECE_SIZE = 0x20000;
        /**
         * The list of files in the directory.
         */
        private final File[]        files;
        /**
         * The index of the next pathname to use.
         */
        private int                 index;
        /**
         * Iterator over the pieces of data in a file.
         */
        private Iterator<PieceInfo> pieceIterator;
        /**
         * Iterator over notices from a sub-directory.
         */
        private FileIterator        subIterator;
        /**
         * The pathname of the root of the directory tree.
         */
        private final File          root;
        /**
         * The next notice to return.
         */
        private Notice              notice;

        /**
         * Constructs from the pathname of the directory that contains the files
         * and the rootPath of the directory tree.
         * 
         * @param dir
         *            The pathname of the directory that contains the files.
         * @param root
         *            The pathname of the root of the directory tree.
         * @throws NullPointerException
         *             if {@code root == null}.
         */
        FileIterator(final File dir, final File root) {
            if (null == root) {
                throw new NullPointerException();
            }
            this.root = root;
            files = dir.listFiles();
            setNextNotice();
        }

        /**
         * Sets the next notice to be returned. Sets it to {@code null} if there
         * are no more notices.
         */
        private void setNextNotice() {
            if (null != pieceIterator && pieceIterator.hasNext()) {
                notice = new PieceNotice(pieceIterator.next());
                return;
            }

            if (null != subIterator && subIterator.hasNext()) {
                notice = subIterator.next();
                return;
            }

            while (files.length > index) {
                final File absFile = files[index++];

                if (absFile.isDirectory()) {
                    subIterator = new FileIterator(absFile, root);

                    if (subIterator.hasNext()) {
                        notice = subIterator.next();
                        return;
                    }
                }
                else {
                    final String absPath = absFile.getPath();

                    if (!absPath.startsWith(root.getPath())) {
                        throw new IllegalStateException("root="
                                + root.getPath() + "\"; absPath=\"" + absPath
                                + "\"");
                    }

                    final File relFile = new File(absPath.substring(root
                            .getPath().length() + 1));
                    final FileInfo fileInfo = new FileInfo(new FileId(relFile),
                            absFile.length(), PIECE_SIZE);

                    notice = new FileNotice(fileInfo);
                    pieceIterator = fileInfo.getPieceInfoIterator();
                    return;
                }
            }

            notice = null;
        }

        @Override
        public boolean hasNext() {
            return null != notice;
        }

        @Override
        public Notice next() {
            if (null == notice) {
                throw new NoSuchElementException();
            }
            final Notice temp = notice;
            setNextNotice();
            return temp;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
