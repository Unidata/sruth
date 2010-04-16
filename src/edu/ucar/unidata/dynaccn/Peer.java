/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;

/**
 * Exchanges data with its remote counterpart.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class Peer implements Callable<Void> {
    /**
     * The task service used by this instance.
     */
    private final ExecutorService                 executorService   = Executors
                                                                            .newCachedThreadPool();
    /**
     * The task completion service used by this instance.
     */
    private final ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<Void>(
                                                                            executorService);
    /**
     * The connection with the remote peer.
     */
    private final Connection                      connection;
    /**
     * Pathname of the directory containing the files to be sent.
     */
    private final File                            outDir;
    /**
     * Pathname of the directory into which to put received files.
     */
    private final File                            inDir;
    /**
     * Request queue.
     */
    private final BlockingQueue<PieceInfo>        noticeQueue       = new SynchronousQueue<PieceInfo>();
    /**
     * Data-piece queue.
     */
    private final BlockingQueue<Piece>            requestQueue      = new SynchronousQueue<Piece>();
    /**
     * Iterator over the files to send to the remote peer.
     */
    final Iterator<Notice>                        noticeIterator;

    /**
     * The futures of the various tasks.
     */
    // private final Future<Void> requestSenderFuture;
    // private final Future<Void> requestReceiverFuture;
    // private final Future<Void> noticeSenderFuture;
    // private final Future<Void> noticeReceiverFuture;
    /**
     * Constructs from a connection to a remote peer.
     * 
     * @param connection
     *            The connection to the remote peer.
     * @param outDir
     *            Pathname of the directory containing files to be sent.
     * @param inDir
     *            Pathname of the directory into which to put received files.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code connection == null || outDir == null || inDir ==
     *             null}.
     */
    Peer(final Connection connection, final File outDir, final File inDir)
            throws IOException {
        if (null == connection || null == outDir || null == inDir) {
            throw new NullPointerException();
        }

        this.connection = connection;
        this.outDir = outDir;
        this.inDir = inDir;
        noticeIterator = new NoticeIterator(outDir, outDir);

        // requestSenderFuture =
        completionService.submit(new RequestSender(this));
        // requestReceiverFuture =
        completionService.submit(new RequestReceiver(this));

        // noticeSenderFuture =
        completionService.submit(new NoticeSender(this, outDir));
        // noticeReceiverFuture =
        completionService.submit(new NoticeReceiver(this));

        completionService.submit(new PieceSender(this));
        completionService.submit(new PieceReceiver(this));
    }

    @Override
    public Void call() throws InterruptedException, ExecutionException {
        try {
            for (int i = 0; i < 4; ++i) {
                completionService.take().get();
            }
        }
        catch (final ExecutionException e) {
            executorService.shutdownNow();
            throw e;
        }
        catch (final CancellationException e) {
            /*
             * Can't happen because cancellation only occurs in the previous
             * exception-handler.
             */
        }

        return null;
    }

    /**
     * Returns the object output stream for notices.
     * 
     * @return The object output stream for notices.
     * @throws IOException
     *             if an I/O error occurs.
     */
    ObjectOutputStream getNoticeOutputStream() throws IOException {
        return connection.getNoticeOutputStream();
    }

    /**
     * Returns the object output stream for requests.
     * 
     * @return The object output stream for requests.
     * @throws IOException
     *             if an I/O error occurs.
     */
    ObjectOutputStream getRequestOutputStream() throws IOException {
        return connection.getRequestOutputStream();
    }

    /**
     * Returns the object input stream for notices.
     * 
     * @return The object input stream for notices.
     * @throws IOException
     *             if an I/O error occurs.
     */
    ObjectInputStream getNoticeInputStream() throws IOException {
        return connection.getNoticeInputStream();
    }

    /**
     * Returns the object input stream for requests.
     * 
     * @return The object input stream for requests.
     * @throws IOException
     *             if an I/O error occurs.
     */
    ObjectInputStream getRequestInputStream() throws IOException {
        return connection.getRequestInputStream();
    }

    /**
     * Returns the object input stream for data-pieces.
     * 
     * @return The object input stream for data-pieces.
     * @throws IOException
     *             if an I/O error occurs.
     */
    ObjectInputStream getDataInputStream() throws IOException {
        return connection.getDataInputStream();
    }

    /**
     * Returns the object output stream for data-pieces.
     * 
     * @return The object output stream for data-pieces.
     * @throws IOException
     *             if an I/O error occurs.
     */
    ObjectOutputStream getDataOutputStream() throws IOException {
        return connection.getDataOutputStream();
    }

    /**
     * Creates an empty file based on information about the file.
     * 
     * @param fileInfo
     *            Information about the file.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void createFile(final FileInfo fileInfo) throws IOException {
        final File file = fileInfo.getFile(inDir);
        final File parent = file.getParentFile();

        parent.mkdirs();
        file.createNewFile();
    }

    /**
     * Processes information about an available piece of data at the remote
     * peer.
     * 
     * @param pieceInfo
     *            Information about the available piece of data at the remote
     *            peer.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    void processNotice(final PieceInfo pieceInfo) throws InterruptedException {
        noticeQueue.put(pieceInfo);
    }

    /**
     * Processes a request for a piece of data from the remote peer.
     * 
     * @param pieceInfo
     *            Information about the piece of data requested by the remote
     *            peer.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void processRequest(final PieceInfo pieceInfo) throws InterruptedException,
            IOException {
        final File path = pieceInfo.getFile(outDir);

        if (!path.exists()) {
            throw new FileNotFoundException("File doesn't exist: \"" + path
                    + "\"");
        }

        final RandomAccessFile file = new RandomAccessFile(path, "r");
        final byte[] data = new byte[pieceInfo.getSize()];

        try {
            file.seek(pieceInfo.getOffset());
            file.read(data);
        }
        finally {
            file.close();
        }

        requestQueue.put(new Piece(pieceInfo, data));
    }

    /**
     * Returns information on the next wanted piece of data.
     * 
     * @return Information on the next wanted piece of data.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    PieceInfo getNextRequest() throws InterruptedException {
        return noticeQueue.take();
    }

    /**
     * Returns the next piece of data to send to the remote peer.
     * 
     * @return The next piece of data to send.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if an I/O error occurs.
     */
    Piece getNextPiece() throws InterruptedException, IOException {
        return requestQueue.take();
    }

    /**
     * Saves a piece of data in the relevant file.
     * 
     * @param piece
     *            The piece to be saved.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void save(final Piece piece) throws IOException {
        final File path = piece.getFile(inDir);

        if (!path.exists()) {
            throw new FileNotFoundException("File doesn't exist: \"" + path
                    + "\"");
        }

        final RandomAccessFile file = new RandomAccessFile(path, "rwd");

        try {
            file.seek(piece.getOffset());
            file.write(piece.getData());
        }
        finally {
            file.close();
        }
    }

    /**
     * Returns the next notice to send to the remote peer.
     * 
     * @return The next notice to send to the remote peer.
     */
    Notice getNextNotice() {
        return noticeIterator.hasNext()
                ? noticeIterator.next()
                : null;
    }

    /**
     * Iterates over the pieces of regular files in a directory. Doesn't
     * recurse.
     * 
     * Instances are thread-compatible but not thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private static class NoticeIterator implements Iterator<Notice> {
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
        private NoticeIterator      subIterator;
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
        NoticeIterator(final File dir, final File root) {
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
        void setNextNotice() {
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
                    subIterator = new NoticeIterator(absFile, root);

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
