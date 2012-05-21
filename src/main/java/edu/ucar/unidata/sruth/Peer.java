/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.file.FileSystemException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;

import edu.ucar.unidata.sruth.ClearingHouse.PieceProcessStatus;
import edu.ucar.unidata.sruth.Connection.Message;
import edu.ucar.unidata.sruth.Connection.Stream;

/**
 * Exchanges data with its remote counterpart.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class Peer implements Callable<Boolean> {
    /**
     * The logging service.
     */
    private static final Logger        logger             = Util.getLogger();
    /**
     * The data clearing-house to use
     */
    private final ClearingHouse        clearingHouse;
    /**
     * The connection with the remote peer.
     */
    private final Connection           connection;
    /**
     * Notice queue. Contains notices of data to be sent to the remote peer.
     */
    private final NoticeQueue          noticeQueue        = new NoticeQueue();
    /**
     * Piece queue. Contains pieces of data to be sent to the remote peer.
     */
    private final BlockingQueue<Piece> pieceQueue         = new SynchronousQueue<Piece>();
    /**
     * Specification of data desired by the local peer.
     */
    private final Filter               localFilter;
    /**
     * Specification of data desired by the remote peer.
     */
    private final Filter               remoteFilter;
    /**
     * Request queue. Contains specifications of data-pieces to request from the
     * remote peer.
     */
    private final DataSpecQueue        requestQueue       = new DataSpecQueue();
    /**
     * The executor service for all the threads of a peer.
     */
    private final CancellingExecutor   cancellingExecutor = new CancellingExecutor(
                                                                  0,
                                                                  Integer.MAX_VALUE,
                                                                  0L,
                                                                  TimeUnit.SECONDS,
                                                                  new SynchronousQueue<Runnable>());
    /**
     * A counter of the amount of downloaded data.
     */
    private volatile long              counter;
    /**
     * Whether or not the counter is stopped.
     */
    private volatile boolean           counterStopped;

    /**
     * Constructs from the pathname of the root of the file-tree and a
     * connection to a remote peer. Accepts responsibility for eventually
     * closing the connection.
     * 
     * @param clearingHouse
     *            The data clearing-house to use.
     * @param connection
     *            The connection to the remote peer. Will be closed by
     *            {@link #call()}.
     * @param localFilter
     *            The specification of data that this instance wants.
     * @param remoteFilter
     *            The specification of data that the remote peer wants.
     * @throws NullPointerException
     *             if {@code clearingHouse == null || connection == null}.
     */
    Peer(final ClearingHouse clearingHouse, final Connection connection,
            final Filter localFilter, final Filter remoteFilter) {
        if (null == clearingHouse) {
            throw new NullPointerException();
        }
        if (null == connection) {
            throw new NullPointerException();
        }
        if (null == localFilter) {
            throw new NullPointerException();
        }
        if (null == remoteFilter) {
            throw new NullPointerException();
        }

        this.clearingHouse = clearingHouse;
        this.connection = connection;
        this.localFilter = localFilter;
        this.remoteFilter = remoteFilter;
    }

    /**
     * Returns this instance's {@link Connection}.
     * 
     * @return This instance's connection.
     */
    Connection getConnection() {
        return connection;
    }

    /**
     * Returns the filter for locally-desired data.
     * 
     * @return the filter for locally-desired data.
     */
    Filter getLocalFilter() {
        return localFilter;
    }

    /**
     * Returns the address of the socket of the server associated with the
     * remote end.
     * 
     * @return The socket address of the server at the remote end.
     */
    InetSocketAddress getRemoteServerSocketAddress() {
        return connection.getRemoteServerSocketAddress();
    }

    /**
     * Executes this instance. Upon completion, the connection is closed.
     * <p>
     * This is a potentially indefinite operation.
     * 
     * @return {@code false} if this instance duplicates a previously existing
     *         one. {@code true} if all locally-desired data was received.
     * @throws EOFException
     *             if the connection was closed by the remote peer.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if a serious I/O error occurs.
     * @throws SocketException
     *             if the connection was closed by the remote peer.
     * @throws IllegalStateException
     *             if a logic error exists
     */
    @Override
    public final Boolean call() throws EOFException, IOException,
            SocketException, InterruptedException {
        logger.trace("Starting up: {}", this);
        final String origName = Thread.currentThread().getName();
        Thread.currentThread().setName(toString());
        boolean validPeer;

        try {
            final CompletionService<Void> completionService = new ExecutorCompletionService<Void>(
                    cancellingExecutor);

            try {
                if (remoteFilter.equals(Filter.NOTHING)) {
                    // The remote instance doesn't want data
                    connection.getNoticeStream().getOutput().close();
                    connection.getRequestStream().getInput().close();
                    connection.getDataStream().getOutput().close();
                }
                else {
                    // The remote instance wants data
                    completionService.submit(new NoticeSender(connection
                            .getNoticeStream()));
                    completionService.submit(new RequestReceiver(connection
                            .getRequestStream()));
                    completionService.submit(new PieceSender(connection
                            .getDataStream()));
                }

                if (localFilter.equals(Filter.NOTHING)) {
                    // This instance doesn't want data
                    connection.getNoticeStream().getInput().close();
                    connection.getRequestStream().getOutput().close();
                    connection.getDataStream().getInput().close();
                }
                else {
                    // This instance wants data
                    completionService.submit(new NoticeReceiver(connection
                            .getNoticeStream()));
                    completionService.submit(new RequestSender(connection
                            .getRequestStream()));
                    completionService.submit(new PieceReceiver(connection
                            .getDataStream()));
                }

                validPeer = clearingHouse.add(this);
                if (!validPeer) {
                    logger.debug("Not a valid peer: {}", this);
                }
                else {
                    try {
                        Future<Void> fileScannerFuture = null;
                        if (!remoteFilter.equals(Filter.NOTHING)) {
                            // The remote instance wants data
                            fileScannerFuture = completionService
                                    .submit(new FileScanner());
                        }

                        for (Future<Void> future = completionService.take(); !future
                                .isCancelled(); future = completionService
                                .take()) {
                            try {
                                future.get();
                            }
                            catch (final ExecutionException e) {
                                final Throwable cause = e.getCause();
                                logger.debug(cause.toString());
                                if (cause instanceof EOFException) {
                                    throw (EOFException) cause;
                                }
                                if (cause instanceof SocketTimeoutException) {
                                    throw (SocketTimeoutException) cause;
                                }
                                if (cause instanceof SocketException) {
                                    throw (SocketException) cause;
                                }
                                if (cause instanceof IOException) {
                                    throw (IOException) cause;
                                }
                                throw Util.launderThrowable(cause);
                            }
                            if (!future.equals(fileScannerFuture)) {
                                // A Sender or Receiver completed
                                logger.trace(
                                        "Sender or Receiver completed: {}",
                                        this);
                                break;
                            }
                        }
                    }
                    finally {
                        clearingHouse.remove(this);
                    }
                }
            }
            finally {
                cancellingExecutor.shutdownNow();
                Thread.interrupted();
                cancellingExecutor.awaitTermination(Long.MAX_VALUE,
                        TimeUnit.DAYS);
            }
        }
        finally {
            connection.close();
            Thread.currentThread().setName(origName);
            logger.trace("Done: {}", this);
        }

        return new Boolean(validPeer);
    }

    /**
     * Notifies the remote peer of a piece of available data if it's desired by
     * the remote peer.
     * 
     * @param pieceSpec
     *            Information on the piece of data.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    void notifyRemoteIfDesired(final PieceSpec pieceSpec)
            throws InterruptedException {
        if (remoteFilter.matches(pieceSpec.getArchivePath())) {
            noticeQueue.put(pieceSpec);
        }
    }

    /**
     * Adds a request to be made of the remote peer.
     * 
     * @param pieceSpec
     *            Specification of the piece of data to request from the remote
     *            peer.
     */
    void addRequest(final PieceSpec pieceSpec) {
        requestQueue.put(pieceSpec);
    }

    /**
     * Handles the creation of new local data by putting a notice in the
     * notice-queue for the remote peer, if appropriate.
     * 
     * @param spec
     *            Specification of the new data.
     */
    void newData(final FilePieceSpecSet spec) {
        logger.trace("New data: {}", spec);
        if (remoteFilter.matches(spec.getArchivePath())) {
            noticeQueue.put(spec);
        }
    }

    /**
     * Responds to the removal of a local file or category by putting a
     * removal-notice in the notice-queue for the remote peer.
     * 
     * @param fileId
     *            Identifier of the file or category.
     */
    void notifyRemoteOfRemovals(final FileId fileId) {
        noticeQueue.put(fileId);
    }

    /**
     * Removes a file.
     * 
     * @param fileId
     *            Identifier of the file.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void remove(final FileId fileId) throws IOException {
        clearingHouse.remove(fileId);
    }

    /**
     * Sets the name of the current thread.
     */
    private static void setThreadName(final String className) {
        final Thread currentThread = Thread.currentThread();
        currentThread.setName(className);
    }

    /**
     * Processes a notice of available data at the remote peer.
     * 
     * @param pieceSpec
     *            Specification of the available data.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void newRemoteData(final PieceSpec pieceSpec) throws IOException {
        try {
            clearingHouse.process(this, pieceSpec);
        }
        catch (final FileInfoMismatchException e) {
            logger.warn("Mismatched file-information: {}: {}", e.toString(),
                    this);
        }
    }

    /**
     * Stops the counter of downloaded data.
     */
    void stopCounter() {
        counterStopped = true;
    }

    /**
     * Returns the amount of downloaded data since the most recent call to
     * {@link #call()} or {@link #stopCounter()}.
     * 
     * @return The amount of downloaded data in octets.
     */
    long getCounter() {
        return counter;
    }

    /**
     * Resets and restarts the counter.
     */
    void restartCounter() {
        counter = 0;
        counterStopped = false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Peer [connection=" + connection + ", remoteFilter="
                + remoteFilter + "]";
    }

    /**
     * Scans the archive for remotely-desired data and adds it to the notice
     * queue.
     * 
     * @author Steven E. Emmerson
     */
    @ThreadSafe
    private final class FileScanner implements Callable<Void> {
        public Void call() throws InterruptedException, IOException {
            setThreadName(toString());
            logger.trace("Starting up: {}", this);
            clearingHouse.walkArchive(new FilePieceSpecSetConsumer() {
                @Override
                public void consume(final FilePieceSpecSet spec) {
                    noticeQueue.put(spec);
                }
            }, remoteFilter);
            return null;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return "FileScanner [rootDir=" + clearingHouse.getRootDir() + "]";
        }
    }

    /**
     * Receives objects from the remote peer and acts upon them.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private static abstract class Receiver<T> extends UninterruptibleTask<Void> {
        /**
         * The underlying {@link Connection#Stream}.
         */
        private final Connection.Stream.Input stream;
        /**
         * The type of the received objects.
         */
        private final Class<T>                type;

        /**
         * Constructs from a stream with the remote peer, and the type of
         * received objects.
         * 
         * @param stream
         *            The stream with the remote peer.
         * @param type
         *            The type of the received objects.
         * @throws NullPointerException
         *             if {stream == null || type == null}.
         */
        protected Receiver(final Connection.Stream stream, final Class<T> type) {
            if (null == type) {
                throw new NullPointerException();
            }

            this.stream = stream.getInput();
            this.type = type;
        }

        /**
         * Reads objects from the connection with the remote peer and processes
         * them. Will block until the connection is initialized by the remote
         * peer. Completes normally if and only if 1) the connection is closed;
         * or 2) the subclass indicates that no more processing should occur
         * (because all the desired data has been received, for example). Closes
         * the input stream.
         * 
         * @throws ClassCastException
         *             if a received object has the wrong type.
         * @throws ClassNotFoundException
         *             if a received object has unknown type.
         * @throws EOFException
         *             if a read on the connection returned EOF.
         * @throws IOException
         *             if a serious I/O error occurs.
         * @throws SocketException
         *             if the connection was closed by the remote peer.
         */
        public Void call() throws EOFException, IOException, SocketException,
                ClassNotFoundException {
            final String origName = Thread.currentThread().getName();
            setThreadName(toString());
            try {
                for (;;) {
                    final Object obj = readObject();
                    if (!process(type.cast(obj))) {
                        break;
                    }
                }
            }
            catch (final InterruptedException ignored) {
                logger.debug("Interrupted: {}", getClass().getSimpleName());
            }
            catch (final IOException e) {
                if (isCancelled()) {
                    logger.debug("Interrupted: {}", getClass().getSimpleName());
                }
                else {
                    throw e;
                }
            }
            finally {
                stream.close();
                Thread.currentThread().setName(origName);
            }
            return null;
        }

        /**
         * Returns the next object from an object input stream or {@code null}
         * if no more objects are forthcoming.
         * 
         * @param ois
         *            The object input stream.
         * @return The next object from the object input stream or {@code null}.
         * @throws EOFException
         *             if a read on the connection returned EOF.
         * @throws ClassCastException
         *             if the object has the wrong type.
         * @throws ClassNotFoundException
         *             if object has unknown type.
         * @throws IOException
         *             if a serious I/O error occurs.
         * @throws SocketException
         *             if the connection was closed by the remote peer.
         */
        protected final Object readObject() throws EOFException, IOException,
                ClassNotFoundException, SocketException {
            try {
                final Object obj = stream.receiveObject(0);
                return obj;
            }
            catch (final SocketTimeoutException impossible) {
                throw new AssertionError(impossible);
            }
        }

        /**
         * Processes an object.
         * 
         * @param obj
         *            The object to process.
         * @return {@code true} if and only if processing should continue.
         * @throws FileSystemException
         *             if too many files are open.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         * @throws IOException
         *             if an I/O error occurs.
         */
        protected abstract boolean process(T obj) throws IOException,
                InterruptedException;

        /**
         * Stops this instance by closing the relevant {@link Connection#Stream}
         * .
         */
        @Override
        protected final void stop() {
            stream.close();
        }
    }

    /**
     * Receives requests for data and processes them.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private final class RequestReceiver extends Receiver<PieceSpecSet> {
        /**
         * Constructs from a stream to the remote peer.
         * 
         * @param stream
         *            The stream to the remote peer.
         * @throws NullPointerException
         *             if {stream == null}.
         */
        RequestReceiver(final Stream stream) {
            super(stream, PieceSpecSet.class);
        }

        @Override
        protected boolean process(final PieceSpecSet request)
                throws InterruptedException, IOException {
            for (final PieceSpec spec : request) {
                try {
                    final Piece piece = clearingHouse.getPiece(spec);
                    if (piece != null) {
                        pieceQueue.put(piece);
                    }
                }
                catch (final FileInfoMismatchException e) {
                    logger.warn("Mismatched file-information: {}: {}",
                            e.toString(), this);
                }
            }
            return true;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return "RequestReceiver [peer=" + Peer.this + "]";
        }
    }

    /**
     * Receives notices of data and processes them
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private final class NoticeReceiver extends Receiver<Notice> {
        /**
         * Constructs from a stream to the remote peer.
         * 
         * @param stream
         *            The stream to the remote peer.
         * @throws NullPointerException
         *             if {stream == null}.
         */
        NoticeReceiver(final Stream stream) {
            super(stream, Notice.class);
        }

        @Override
        protected boolean process(final Notice notice) throws IOException,
                InterruptedException {
            notice.processYourself(Peer.this);
            return true;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return "NoticeReceiver []";
        }
    }

    /**
     * Receives pieces of data and processes them.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private final class PieceReceiver extends Receiver<Piece> {
        /**
         * Constructs from a stream to the remote peer.
         * 
         * @param stream
         *            The stream to the remote peer.
         * @throws NullPointerException
         *             if {stream == null}.
         */
        PieceReceiver(final Stream stream) {
            super(stream, Piece.class);
        }

        @Override
        protected boolean process(final Piece piece) throws IOException,
                InterruptedException {
            try {
                final PieceProcessStatus status = clearingHouse.process(
                        Peer.this, piece);
                if (!counterStopped && status.wasUsed()) {
                    counter += piece.getSize();
                }
                return !status.isDone();
            }
            catch (final FileInfoMismatchException e) {
                logger.warn("Mismatched file-information: {}: {}",
                        e.toString(), this);
                return true;
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return "PieceReceiver [peer=" + Peer.this + "]";
        }
    }

    /**
     * Sends objects to the remote peer.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private static abstract class Sender extends UninterruptibleTask<Void> {
        /**
         * The underlying {@link Connection#Stream}.
         */
        private final Connection.Stream.Output stream;

        /**
         * Constructs from a stream to the remote peer.
         * 
         * @param stream
         *            The relevant stream to the remote peer.
         * @throws NullPointerException
         *             if {stream == null}.
         */
        protected Sender(final Connection.Stream stream) {
            this.stream = stream.getOutput();
        }

        /**
         * Executes this instance. Completes normally if and only if 1) the
         * {@link #nextObject()} method returns {@code null}; 2) the underlying
         * connection is closed; or 3) the current thread is interrupted. Closes
         * the output stream.
         * 
         * @throws IOException
         *             if an I/O error occurs.
         */
        @Override
        public final Void call() throws IOException {
            setThreadName(toString());
            try {
                for (Message obj = nextObject(); null != obj; obj = nextObject()) {
                    writeObject(obj);
                }
            }
            catch (final SocketException e) {
                if (!isCancelled()) {
                    logger.debug("Stream closed: {}", stream);
                    throw e;
                }
            }
            catch (final InterruptedException ignored) {
                logger.debug("Interrupted: {}", getClass().getSimpleName());
            }
            finally {
                stream.close();
            }
            return null;
        }

        /**
         * Writes an object to an object output stream. This method should be
         * used for sending all objects to the remote peer -- including any sent
         * by {@link #initialize(ObjectOutputStream)}.
         * 
         * @param obj
         *            The object to be written.
         * @param oos
         *            The object output stream.
         * @throws IOException
         *             if an I/O error occurs.
         */
        protected final void writeObject(final Message obj) throws IOException {
            stream.send(obj);
        }

        /**
         * Returns the next object to send.
         * 
         * @return The next object to send.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         */
        protected abstract Message nextObject() throws InterruptedException;

        /**
         * Stops the thread executing this instance by closing the output
         * stream.
         */
        @Override
        protected final void stop() {
            stream.close();
        }
    }

    /**
     * Sends requests for data to the remote peer after initially sending the
     * specification of locally-desired data.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private final class RequestSender extends Sender {
        /**
         * Constructs from a stream to the remote peer.
         * 
         * @param stream
         *            The stream to the remote peer.
         * @throws NullPointerException
         *             if {stream == null}.
         */
        RequestSender(final Stream stream) {
            super(stream);
        }

        @Override
        public PieceSpecSet nextObject() throws InterruptedException {
            return requestQueue.take();
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return "RequestSender []";
        }
    }

    /**
     * Sends notices of available data to a remote peer.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private final class NoticeSender extends Sender {
        /**
         * Constructs from a stream to a remote peer and a specification of the
         * data desired by the remote peer.
         * 
         * @param stream
         *            The stream to the remote peer.
         * @throws NullPointerException
         *             if {stream == null}.
         */
        NoticeSender(final Stream stream) {
            super(stream);
        }

        @Override
        protected Notice nextObject() throws InterruptedException {
            final Notice notice = noticeQueue.take();
            return notice;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return "NoticeSender []";
        }
    }

    /**
     * Sends pieces of data to the remote peer.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private final class PieceSender extends Sender {
        /**
         * Constructs from a stream to the remote peer.
         * 
         * @param stream
         *            The stream to the remote peer.
         * @throws NullPointerException
         *             if {stream == null}.
         */
        PieceSender(final Stream stream) {
            super(stream);
        }

        @Override
        protected Piece nextObject() throws InterruptedException {
            final Piece piece = pieceQueue.take();
            return piece;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return "PieceSender []";
        }
    }

    /**
     * A queue of data-specifications.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private static final class DataSpecQueue {
        /**
         * The set of piece-specifications.
         */
        @GuardedBy("this")
        private PieceSpecSet pieceSpecSet;

        /**
         * Adds a data-specification.
         * 
         * @param spec
         *            The specification of the data to be added.
         */
        synchronized void put(final FilePieceSpecSet spec) {
            logger.trace("New data: {}", spec);
            pieceSpecSet = (null == pieceSpecSet)
                    ? spec
                    : pieceSpecSet.merge(spec);
            notify();
        }

        /**
         * Indicates if this instance has a specification or not.
         * 
         * @return {@code true} if and only if this instance has a
         *         specification.
         */
        synchronized boolean isEmpty() {
            return pieceSpecSet == null || pieceSpecSet.isEmpty();
        }

        /**
         * Returns the next data-specification. Blocks until one is available.
         * 
         * @return The next data-specification.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         */
        synchronized PieceSpecSet take() throws InterruptedException {
            while (isEmpty()) {
                wait();
            }
            return removeAndReturn();
        }

        /**
         * Removes and returns the next data-specification of one exists;
         * otherwise, returns {@code null}.
         * 
         * @return The next data-specification of one exists; otherwise,
         *         {@code null}.
         */
        synchronized PieceSpecSet poll() {
            if (isEmpty()) {
                return null;
            }
            return removeAndReturn();
        }

        @GuardedBy("this")
        private PieceSpecSet removeAndReturn() {
            final PieceSpecSet specs = pieceSpecSet;
            pieceSpecSet = null;
            return specs;
        }
    }

    /**
     * A queue of notices.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private static final class NoticeQueue {
        /**
         * Was the last notice an addition?
         */
        private volatile boolean    wasAddition = false;
        /**
         * The queue of additions.
         */
        @GuardedBy("this")
        private final DataSpecQueue additions   = new DataSpecQueue();
        /**
         * The queue of removals.
         */
        @GuardedBy("this")
        private FileSetSpec         removals    = new FileSetSpec();

        /**
         * Adds a notice of new data.
         * 
         * @param spec
         *            The specification of the data to be added.
         */
        synchronized void put(final FilePieceSpecSet spec) {
            additions.put(spec);
            notify();
        }

        /**
         * Adds a notice of removal of a file.
         * 
         * @param fileId
         *            The identifier of the file.
         */
        synchronized void put(final FileId fileId) {
            removals.add(fileId);
            notify();
        }

        /**
         * Returns the next notice. Blocks until one is available.
         * 
         * @return The next notice.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         */
        synchronized Notice take() throws InterruptedException {
            Notice notice;
            while (removals.isEmpty() && additions.isEmpty()) {
                wait();
            }
            if (additions.isEmpty() || (!removals.isEmpty() && wasAddition)) {
                notice = (1 == removals.size())
                        ? new RemovedFileNotice(removals.iterator().next())
                        : new RemovedFilesNotice(removals);
                removals = new FileSetSpec();
                wasAddition = false;
            }
            else {
                notice = new AdditionNotice(additions.poll());
                wasAddition = true;
            }
            return notice;
        }
    }
}