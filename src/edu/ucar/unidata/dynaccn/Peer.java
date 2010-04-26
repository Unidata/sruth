/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
    private final ExecutorService                 executorService      = Executors
                                                                               .newCachedThreadPool();
    /**
     * The task completion service used by this instance.
     */
    private final ExecutorCompletionService<Void> completionService    = new ExecutorCompletionService<Void>(
                                                                               executorService);
    /**
     * The connection with the remote peer.
     */
    private final Connection                      connection;
    /**
     * Pathname of the root of the file hierarchy.
     */
    private final File                            dir;
    /**
     * Request queue. Contains specifications of data-pieces to request from the
     * remote peer.
     */
    private final BlockingQueue<Request>          requestQueue         = new ArrayBlockingQueue<Request>(
                                                                               1);
    /**
     * Piece queue. Contains pieces of data to be sent to the remote peer.
     */
    private final BlockingQueue<Piece>            pieceQueue           = new SynchronousQueue<Piece>();
    /**
     * Iterator over the files to send to the remote peer.
     */
    private final Iterator<Notice>                noticeIterator;
    /**
     * The predicate for selecting locally-desired data.
     */
    private final Predicate                       predicate;
    /**
     * Whether or not this instance is done.
     */
    private final AtomicBoolean                   done                 = new AtomicBoolean(
                                                                               false);
    /**
     * Rendezvous-queue for the remotely-desired data.
     */
    private final SynchronousQueue<Predicate>     remotePredicateQueue = new SynchronousQueue<Predicate>();
    /**
     * Specification of data desired by the remote peer.
     */
    private final AtomicReference<Predicate>      remotePredicate      = new AtomicReference<Predicate>();

    /**
     * Constructs from a connection to a remote peer.
     * 
     * @param connection
     *            The connection to the remote peer.
     * @param dir
     *            Pathname of the root of the file hierarchy.
     * @param predicate
     *            Predicate for selecting locally-desired data.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code connection == null || dir == null || predicate ==
     *             null}.
     */
    Peer(final Connection connection, final File dir, final Predicate predicate)
            throws IOException {
        if (null == connection || null == dir || predicate == null) {
            throw new NullPointerException();
        }

        this.connection = connection;
        this.dir = dir;
        noticeIterator = new NoticeIterator(dir, dir);
        this.predicate = predicate;
    }

    /**
     * Executes this instance and waits upon one of the following conditions: 1)
     * this instance receives data and all data that can be received has been
     * received; 2) an error occurs; or 3) the current thread is interrupted. In
     * any case, any and all subtasks will have been terminated upon return and
     * the connection with the remote peer will be closed.
     * 
     * @throws ExecutionException
     *             if this instance terminated due to an error in a subtask.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if an I/O error occurs.
     */
    @Override
    public Void call() throws InterruptedException, ExecutionException,
            IOException {
        try {
            /*
             * Prime the request-queue with a specification of the desired data.
             */
            requestQueue.put(new PredicateRequest(predicate));

            completionService.submit(new RequestSender(connection));
            completionService.submit(new RequestReceiver(connection));

            // The notice-sender needs the remote predicate
            remotePredicate.set(remotePredicateQueue.take());

            completionService.submit(new NoticeSender(connection,
                    remotePredicate.get()));
            completionService.submit(new NoticeReceiver(connection));

            completionService.submit(new PieceSender(connection));
            completionService.submit(new PieceReceiver(connection));

            for (int i = 0; i < 6; ++i) {
                try {
                    final Future<Void> future = completionService.take();

                    if (!future.isCancelled()) {
                        try {
                            future.get();

                            if (done.get()) {
                                executorService.shutdownNow();
                                break;
                            }
                        }
                        catch (final ExecutionException e) {
                            executorService.shutdownNow();
                            throw e;
                        }
                        catch (final CancellationException e) {
                            // Can't happen
                        }
                    }
                }
                catch (final InterruptedException e) {
                    executorService.shutdownNow();
                    throw e;
                }
            }
        }
        finally {
            connection.close();
        }

        return null;
    }

    /**
     * Processes a specification of desired-data from the remote peer.
     * 
     * @param predicate
     *            The specification of data desired by the remote peer.
     */
    public void processRemotelyDesiredDataSpecification(
            final Predicate predicate) {
        remotePredicateQueue.add(predicate);
    }

    /**
     * Returns the next notice to send to the remote peer.
     * 
     * @return The next notice to send to the remote peer.
     */
    Notice getNextNotice() {
        final Predicate predicate = remotePredicate.get();
        while (noticeIterator.hasNext()) {
            final Notice notice = noticeIterator.next();
            if (predicate.satisfiedBy(notice.getFileInfo())) {
                return notice;
            }
        }
        return null;
    }

    /**
     * Processes a notice about a file that's available from the remote peer.
     * 
     * @param fileInfo
     *            Information about the file.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void processNotice(final FileInfo fileInfo) throws IOException {
        if (predicate.satisfiedBy(fileInfo)) {
            DiskFile.create(dir, fileInfo);
        }
    }

    /**
     * Processes a notice about a piece of data that's available from the remote
     * peer.
     * 
     * @param pieceInfo
     *            Information about the piece of data at the remote peer.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    void processNotice(final PieceInfo pieceInfo) throws InterruptedException {
        if (predicate.satisfiedBy(pieceInfo.getFileInfo())) {
            requestQueue.put(new PieceRequest(pieceInfo));
        }
    }

    /**
     * Returns the next request for a piece of data to send to the remote peer.
     * 
     * @return Information on the next wanted piece of data.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    Request getNextRequest() throws InterruptedException {
        return requestQueue.take();
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
        pieceQueue.put(DiskFile.getPiece(dir, pieceInfo));
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
        return pieceQueue.take();
    }

    /**
     * Processes a piece of data from the remote peer.
     * 
     * @param piece
     *            The piece of data from the remote peer.
     * @return {@code true} if and only more data is desired.
     * @throws IOException
     *             if an I/O error occurs.
     */
    boolean processData(final Piece piece) throws IOException {
        if (!predicate.satisfiedBy(piece.getFileInfo())) {
            // Server-created peers always do this
            return true; // continue
        }

        if (DiskFile.putPiece(dir, piece)) {
            predicate.removeIfPossible(piece.getFileInfo());
        }

        if (predicate.isEmpty()) {
            done.set(true);
            return false; // terminate
        }

        return true; // continue
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

    /**
     * Receives objects from the remote peer and acts upon them.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private abstract class Receiver<T> implements Callable<Void> {
        /**
         * The input stream from the remote peer.
         */
        private final InputStream inputStream;
        /**
         * The type of the received objects.
         */
        private final Class<T>    type;

        /**
         * Constructs from an input stream from the remote peer.
         * 
         * @param inputStream
         *            The input stream from the remote peer.
         * @param type
         *            The type of the received objects.
         * @throws NullPointerException
         *             if {inputStream == null}.
         */
        protected Receiver(final InputStream inputStream, final Class<T> type) {
            if (null == inputStream || null == type) {
                throw new NullPointerException();
            }

            this.inputStream = inputStream;
            this.type = type;
        }

        /**
         * Reads objects from the connection with the remote peer and processes
         * them. Will block until the connection is initialized by the remote
         * peer. Returns normally if and only if an end-of-file is read or the
         * subclass indicates that no more processing should occur.
         * 
         * @throws ClassCastException
         *             if the object has the wrong type.
         * @throws ClassNotFoundException
         *             if object has unknown type.
         * @throws IOException
         *             if an I/O error occurs.
         */
        public Void call() throws IOException, ClassNotFoundException,
                InterruptedException {
            final ObjectInputStream objectInputStream = new ObjectInputStream(
                    inputStream);
            try {
                for (;;) {
                    final Object obj = objectInputStream.readObject();
                    System.out.println("Receiving: " + obj);
                    if (null != obj) {
                        if (!process(type.cast(obj))) {
                            break;
                        }
                    }
                }
            }
            catch (final EOFException e) {
                // ignored
            }
            return null;
        }

        /**
         * Processes an object.
         * 
         * @return {@code true} if and only if processing should continue.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         * @throws IOException
         *             if an I/O error occurs.
         */
        protected abstract boolean process(T obj) throws IOException,
                InterruptedException;
    }

    /**
     * Receives requests for data and processes them.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private final class RequestReceiver extends Receiver<Request> {
        /**
         * Constructs from a connection to the remote peer. Will block until the
         * remote peer initializes the relevant object stream.
         * 
         * @param connection
         *            The connection to the remote peer.
         * @throws IOException
         *             if an I/O error occurs.
         * @throws NullPointerException
         *             if {connection == null}.
         */
        RequestReceiver(final Connection connection) throws IOException {
            super(connection.getRequestInputStream(), Request.class);
        }

        @Override
        protected boolean process(final Request request)
                throws InterruptedException, IOException {
            request.process(Peer.this);
            return true;
        }
    }

    /**
     * Receives notices of data and processes them
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private final class NoticeReceiver extends Receiver<Notice> {
        /**
         * Constructs from a connection to the remote peer.
         * 
         * @param connection
         *            The connection to the remote peer.
         * @throws IOException
         *             if an I/O error occurs.
         * @throws NullPointerException
         *             if {connection == null}.
         */
        NoticeReceiver(final Connection connection) throws IOException {
            super(connection.getNoticeInputStream(), Notice.class);
        }

        @Override
        protected boolean process(final Notice notice) throws IOException,
                InterruptedException {
            notice.process(Peer.this);
            return true;
        }
    }

    /**
     * Receives pieces of data and processes them.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private final class PieceReceiver extends Receiver<Piece> {
        /**
         * Constructs from a connection to the remote peer.
         * 
         * @param connection
         *            The connection to the remote peer.
         * @throws IOException
         *             if an I/O error occurs.
         * @throws NullPointerException
         *             if {connection == null}.
         */
        PieceReceiver(final Connection connection) throws IOException {
            super(connection.getDataInputStream(), Piece.class);
        }

        @Override
        protected boolean process(final Piece piece) throws IOException {
            if (!predicate.satisfiedBy(piece.getFileInfo())) {
                // Server-created peers always do this
                return true; // continue
            }
            if (DiskFile.putPiece(dir, piece)) {
                predicate.removeIfPossible(piece.getFileInfo());
            }
            if (predicate.isEmpty()) {
                done.set(true);
                return false; // terminate
            }
            return true; // continue
        }
    }

    /**
     * Sends objects to the remote peer.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private abstract class Sender<T> implements Callable<Void> {
        /**
         * The output stream to the remote peer.
         */
        private final OutputStream outputStream;

        /**
         * Constructs from a local peer and the output stream to the remote
         * peer.
         * 
         * @param peer
         *            The local peer.
         * @param outputStream
         *            The outputStream to the remote peer.
         * @throws NullPointerException
         *             if {peer == null || objectOutputStream == null}.
         */
        protected Sender(final OutputStream outputStream) {
            if (null == outputStream) {
                throw new NullPointerException();
            }

            this.outputStream = outputStream;
        }

        /**
         * Executes this instance. Returns normally if and only if the
         * {@link #nextObject} method returns {@code null}.
         * 
         * @throws InterruptedException
         *             if the current thread is interrupted.
         * @throws IOException
         *             if an I/O error occurs.
         */
        @Override
        public final Void call() throws IOException, InterruptedException {
            final ObjectOutputStream objectOutputStream = new ObjectOutputStream(
                    outputStream);
            for (;;) {
                final T obj = nextObject();
                if (null == obj) {
                    break;
                }
                System.out.println("Sending: " + obj);
                objectOutputStream.writeObject(obj);
                objectOutputStream.flush();
            }
            return null;
        }

        /**
         * Returns the next object to send.
         * 
         * @return The next object to send.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         * @throws IOException
         *             if an I/O error occurs.
         */
        protected abstract T nextObject() throws InterruptedException,
                IOException;
    }

    /**
     * Sends requests for data to the remote peer.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private final class RequestSender extends Sender<Request> {
        /**
         * Constructs from a peer and the connection to the remote peer.
         * 
         * @param peer
         *            The local peer.
         * @param connection
         *            The connection to the remote peer.
         * @throws IOException
         *             if an I/O error occurs.
         * @throws NullPointerException
         *             if {peer == null || connection == null}.
         */
        RequestSender(final Connection connection) throws IOException {
            super(connection.getRequestOutputStream());
        }

        @Override
        public Request nextObject() throws InterruptedException {
            return requestQueue.take();
        }
    }

    /**
     * Sends notices of available data to a remote peer.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private final class NoticeSender extends Sender<Notice> {
        /**
         * Specification of data desired by the remote peer.
         */
        private final Predicate predicate;

        /**
         * Constructs from the connection to a remote peer and a specification
         * of the data desired by the remote peer.
         * 
         * @param Connection
         *            The connection to the remote peer.
         * @param predicate
         *            Specification of data desired by the remote peer.
         * @throws NullPointerException
         *             if {peer == null || connection == null || predicate ==
         *             null}.
         */
        NoticeSender(final Connection connection, final Predicate predicate)
                throws IOException {
            super(connection.getNoticeOutputStream());
            if (null == predicate) {
                throw new NullPointerException();
            }
            this.predicate = predicate;
        }

        @Override
        protected Notice nextObject() {
            while (noticeIterator.hasNext()) {
                final Notice notice = noticeIterator.next();
                if (predicate.satisfiedBy(notice.getFileInfo())) {
                    return notice;
                }
            }
            return null;
        }
    }

    /**
     * Sends pieces of data to the remote peer.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private final class PieceSender extends Sender<Piece> {
        /**
         * Constructs from a connection to the remote peer.
         * 
         * @param Connection
         *            The connection to the remote peer.
         * @throws IOException
         *             if an I/O error occurs.
         * @throws NullPointerException
         *             if {peer == null || connection == null}.
         */
        PieceSender(final Connection connection) throws IOException {
            super(connection.getDataOutputStream());
        }

        @Override
        protected Piece nextObject() throws InterruptedException, IOException {
            return pieceQueue.take();
        }
    }

    /**
     * An object on the request-stream.
     * 
     * Instances are immutable.
     * 
     * @author Steven R. Emmerson
     */
    private static abstract class Request implements Serializable {
        /**
         * Serial version ID.
         */
        private static final long serialVersionUID = 1L;

        /**
         * Processes the request via the local peer.
         * 
         * @param peer
         *            The local peer.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         * @throws IOException
         *             if an I/O error occurs.
         */
        abstract void process(final Peer peer) throws InterruptedException,
                IOException;
    }

    /**
     * A request for a piece of data.
     * 
     * Instances are immutable.
     * 
     * @author Steven R. Emmerson
     */
    private static final class PieceRequest extends Request {
        /**
         * Serial version ID.
         */
        private static final long serialVersionUID = 1L;
        /**
         * Information on the piece of data being requested.
         */
        private final PieceInfo   pieceInfo;

        /**
         * Creates a request for a piece of data.
         * 
         * @param pieceInfo
         *            Information on the piece of data to request.
         * @throws NullPointerException
         *             if {@code pieceInfo == null}.
         */
        PieceRequest(final PieceInfo pieceInfo) {
            if (null == pieceInfo) {
                throw new NullPointerException();
            }

            this.pieceInfo = pieceInfo;
        }

        /**
         * Processes the associated piece-information via the local peer.
         * 
         * @param peer
         *            The local peer.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         * @throws IOException
         *             if an I/O error occurs.
         */
        @Override
        void process(final Peer peer) throws InterruptedException, IOException {
            peer.processRequest(pieceInfo);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{pieceInfo=" + pieceInfo + "}";
        }

        private Object readResolve() throws InvalidObjectException {
            try {
                return new PieceRequest(pieceInfo);
            }
            catch (final Exception e) {
                throw (InvalidObjectException) new InvalidObjectException(
                        "Read invalid " + getClass().getSimpleName())
                        .initCause(e);
            }
        }
    }

    /**
     * A request that specifies the desired data.
     * 
     * Instances are immutable.
     * 
     * @author Steven R. Emmerson
     */
    private static final class PredicateRequest extends Request {
        /**
         * Serial version ID.
         */
        private static final long serialVersionUID = 1L;
        /**
         * The specification of desired data.
         */
        private final Predicate   predicate;

        /**
         * Constructs from a specification of desired data.
         * 
         * @param predicate
         *            Specification of desired data.
         */
        PredicateRequest(final Predicate predicate) {
            if (null == predicate) {
                throw new NullPointerException();
            }
            this.predicate = predicate;
        }

        @Override
        void process(final Peer peer) throws InterruptedException, IOException {
            peer.processRemotelyDesiredDataSpecification(predicate);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{predicate=" + predicate + "}";
        }
    }

    /**
     * A notice of available data.
     * 
     * Instances are immutable.
     * 
     * @author Steven R. Emmerson
     */
    private static abstract class Notice implements Serializable {
        /**
         * The serial version identifier.
         */
        private static final long serialVersionUID = 1L;

        /**
         * Process this instance using the local peer.
         * 
         * @param peer
         *            The local peer.
         * @throws IOException
         *             if an I/O error occurs.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         */
        abstract void process(Peer peer) throws IOException,
                InterruptedException;

        @Override
        public abstract String toString();

        /**
         * Returns the file information associated with this instance.
         * 
         * @return The file information associated with this instance.
         */
        abstract FileInfo getFileInfo();
    }

    /**
     * A notice of an available file.
     * 
     * Instances are immutable.
     * 
     * @author Steven R. Emmerson
     */
    private static final class FileNotice extends Notice {
        /**
         * The serial version identifier.
         */
        private static final long serialVersionUID = 1L;

        /**
         * The associated information on the file.
         */
        private final FileInfo    fileInfo;

        /**
         * Constructs from information on the file.
         * 
         * @param fileInfo
         *            Information on the file
         * @throws NullPointerException
         *             if {@code fileInfo == null}.
         */
        FileNotice(final FileInfo fileInfo) {
            if (null == fileInfo) {
                throw new NullPointerException();
            }

            this.fileInfo = fileInfo;
        }

        @Override
        FileInfo getFileInfo() {
            return fileInfo;
        }

        @Override
        void process(final Peer peer) throws IOException {
            peer.processNotice(fileInfo);
        }

        /*
         * (non-Javadoc)
         * 
         * @see edu.ucar.unidata.dynaccn.Notice#toString()
         */
        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "{fileInfo=" + fileInfo
                    + "}";
        }

        private Object readResolve() throws InvalidObjectException {
            try {
                return new FileNotice(fileInfo);
            }
            catch (final Exception e) {
                throw (InvalidObjectException) new InvalidObjectException(
                        "Read invalid " + getClass().getSimpleName())
                        .initCause(e);
            }
        }
    }

    /**
     * A notice of an available piece of data.
     * 
     * Instances are immutable.
     * 
     * @author Steven R. Emmerson
     */
    private static final class PieceNotice extends Notice {
        /**
         * The serial version identifier.
         */
        private static final long serialVersionUID = 1L;

        /**
         * Information about the associated file-piece.
         */
        private final PieceInfo   pieceInfo;

        /**
         * Constructs from information about a file-piece.
         * 
         * @param pieceInfo
         *            Information of the file-piece.
         * @throws NullPointerException
         *             if {@code pieceInfo == null}.
         */
        PieceNotice(final PieceInfo pieceInfo) {
            if (null == pieceInfo) {
                throw new NullPointerException();
            }

            this.pieceInfo = pieceInfo;
        }

        @Override
        FileInfo getFileInfo() {
            return pieceInfo.getFileInfo();
        }

        @Override
        void process(final Peer peer) throws InterruptedException {
            peer.processNotice(pieceInfo);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{pieceInfo=" + pieceInfo + "}";
        }

        private Object readResolve() throws InvalidObjectException {
            try {
                return new PieceNotice(pieceInfo);
            }
            catch (final Exception e) {
                throw (InvalidObjectException) new InvalidObjectException(
                        "Read invalid " + getClass().getSimpleName())
                        .initCause(e);
            }
        }
    }
}
