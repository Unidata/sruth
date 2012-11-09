/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.EOFException;
import java.io.IOException;
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
     * Notice request queue. Contains specifications of data-pieces about which
     * the remote peer should send notices for those that it has.
     */
    private final DataSpecQueue        requestNoticeQueue = new DataSpecQueue();
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
     * The set of pending data-piece requests (i.e., requests that have been at
     * least queued but whose referenced data-pieces have not yet arrived)
     */
    private final SpecSet              pendingRequests    = new SpecSet();

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
                    /*
                     * The remote instance wants data. Start the tasks in a
                     * particular order.
                     */
                    completionService.submit(new PieceSender(connection));
                    completionService.submit(new RequestReceiver(connection));
                    completionService.submit(new NoticeSender(connection));
                }

                if (localFilter.equals(Filter.NOTHING)) {
                    // This instance doesn't want data
                    connection.getNoticeStream().getInput().close();
                    connection.getRequestStream().getOutput().close();
                    connection.getDataStream().getInput().close();
                }
                else {
                    /*
                     * This instance wants data. Start the tasks in a particular
                     * order.
                     */
                    completionService.submit(new PieceReceiver(connection));
                    completionService.submit(new RequestSender(connection));
                    completionService.submit(new NoticeReceiver(connection));
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
            noticeQueue.newData(pieceSpec);
        }
    }

    /**
     * Queues a request for a data-piece to be made of the remote peer.
     * 
     * @param pieceSpec
     *            Specification of the piece of data to request from the remote
     *            peer.
     */
    void queueRequest(final PieceSpec pieceSpec) {
        pendingRequests.add(pieceSpec);
        requestQueue.put(pieceSpec);
        logger.trace("Request added: {}", pieceSpec);
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
            noticeQueue.newData(spec);
        }
    }

    /**
     * Responds to the removal of a local file or category by putting a
     * removal-notice in the notice-queue for the remote peer.
     * 
     * @param archivePath
     *            Identifier of the file or category.
     */
    void notifyRemoteOfRemovals(final ArchivePath archivePath) {
        noticeQueue.put(archivePath);
    }

    /**
     * Removes a file.
     * 
     * @param archivePath
     *            Archive-pathname of the file.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code archivePath == null}.
     */
    void remove(final ArchivePath archivePath) throws IOException {
        clearingHouse.remove(archivePath);
    }

    /**
     * Queues the specified pieces of data for sending to the remote peer.
     * 
     * @param specs
     *            Specifications of the pieces of data to be queued for sending.
     */
    void queueForSending(final PieceSpecSetIface specs)
            throws InterruptedException, IOException {
        for (final PieceSpec spec : specs) {
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
    }

    /**
     * Queues notices for the data-pieces that are referenced by a set of
     * data-piece specifications and that also exist in the archive.
     * 
     * @param specs
     *            The set of data-piece specifications. The client shall not
     *            modify.
     * @throws FileSystemException
     *             if too many files are open.
     * @throws IOException
     *             if an I/O error occurs
     */
    void queueForSendingNotices(final PieceSpecSetIface specs)
            throws FileSystemException, IOException {
        final Archive archive = clearingHouse.getArchive();
        for (final PieceSpec spec : specs) {
            if (archive.exists(spec)) {
                noticeQueue.newData(spec);
            }
        }
    }

    /**
     * Process a piece of data. May block.
     * 
     * @param piece
     *            The piece of data.
     * @throws IOException
     *             if an I/O error occurs
     * @throws InterruptedException
     *             if the current thread is interrupted
     */
    void process(final Piece piece) throws IOException, InterruptedException {
        try {
            final boolean wasUsed = clearingHouse.process(Peer.this, piece);
            if (!counterStopped && wasUsed) {
                counter += piece.getSize();
            }
        }
        catch (final FileInfoMismatchException e) {
            logger.warn("Mismatched file-information: {}: {}", e.toString(),
                    this);
        }
        pendingRequests.remove(piece.getInfo());
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

    /**
     * Returns the set of pending (i.e., outstanding) requests for data. The
     * actual set is returned -- not a copy.
     * 
     * @return the set of pending (i.e., outstanding) requests for data.
     */
    SpecSet getPendingRequests() {
        return pendingRequests;
    }

    /**
     * Requests the remote peer to send notices of any data-pieces it has that
     * are also in a set of data-piece specifications.
     * 
     * @param specs
     *            The set of data-piece specifications.
     */
    void requestNotices(final SpecSet specs) {
        requestNoticeQueue.put(specs.getSet());
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
                public void consume(final FilePieceSpecSet spec)
                        throws InterruptedException {
                    noticeQueue.oldData(spec);
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
     * Receives messages from the remote peer and processes them.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private abstract class Receiver<T extends PeerMessage> extends
            UninterruptibleTask<Void> {
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
                    final T message = type.cast(obj);
                    message.processYourself(Peer.this);
                    if (allDone()) {
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
         * Indicates if processing of incoming messages should stop.
         * <p>
         * This implementation returns {@code false}.
         */
        protected boolean allDone() {
            return false;
        }

        /**
         * Stops this instance by closing the relevant {@link Connection#Stream}
         * .
         */
        @Override
        protected final void stop() {
            stream.close();
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public final String toString() {
            return getClass().getSimpleName() + " [peer=" + Peer.this + "]";
        }
    }

    /**
     * Receives requests and processes them.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private final class RequestReceiver extends Receiver<Request> {
        /**
         * Constructs from a connection to the remote peer.
         * 
         * @param connection
         *            The connection to the remote peer.
         * @throws NullPointerException
         *             if {connection == null}.
         */
        RequestReceiver(final Connection connection) {
            super(connection.getRequestStream(), Request.class);
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
         * Constructs from a connection to the remote peer.
         * 
         * @param connection
         *            The connection to the remote peer.
         * @throws NullPointerException
         *             if {connection == null}.
         */
        NoticeReceiver(final Connection connection) {
            super(connection.getNoticeStream(), Notice.class);
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
         * Constructs from a connection to the remote peer.
         * 
         * @param connection
         *            The connection to the remote peer.
         * @throws NullPointerException
         *             if {connection == null}.
         */
        PieceReceiver(final Connection connection) {
            super(connection.getDataStream(), Piece.class);
        }

        @Override
        protected boolean allDone() {
            return clearingHouse.allDataReceived();
        }
    }

    /**
     * Sends messages to the remote peer.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private abstract class Sender<T extends PeerMessage> extends
            UninterruptibleTask<Void> {
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
         * {@link #nextMessage()} method returns {@code null}; 2) the underlying
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
                T message;
                while ((message = nextMessage()) != null) {
                    stream.send(message);
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
         * Returns the next message to send.
         * 
         * @return The next message to send.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         */
        protected abstract T nextMessage() throws InterruptedException;

        /**
         * Stops the thread executing this instance by closing the output
         * stream.
         */
        @Override
        protected final void stop() {
            stream.close();
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public final String toString() {
            return getClass().getSimpleName() + " [peer=" + Peer.this + "]";
        }
    }

    /**
     * Sends requests for data to the remote peer.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private final class RequestSender extends Sender<Request> {
        /**
         * Constructs from a connection to the remote peer.
         * 
         * @param connection
         *            The connection to the remote peer.
         * @throws NullPointerException
         *             if {connection == null}.
         */
        RequestSender(final Connection connection) {
            super(connection.getRequestStream());
        }

        @Override
        public PieceRequest nextMessage() throws InterruptedException {
            return new PieceRequest(requestQueue.take());
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
    private final class NoticeSender extends Sender<Notice> {
        /**
         * Constructs from a connection to a remote peer and a specification of
         * the data desired by the remote peer.
         * 
         * @param connection
         *            The connection to the remote peer.
         * @throws NullPointerException
         *             if {connection == null}.
         */
        NoticeSender(final Connection connection) {
            super(connection.getNoticeStream());
        }

        @Override
        protected Notice nextMessage() throws InterruptedException {
            return noticeQueue.take();
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
    private final class PieceSender extends Sender<Piece> {
        /**
         * Constructs from a connection to the remote peer.
         * 
         * @param connection
         *            The connection to the remote peer.
         * @throws NullPointerException
         *             if {connection == null}.
         */
        PieceSender(final Connection connection) {
            super(connection.getDataStream());
        }

        @Override
        protected Piece nextMessage() throws InterruptedException {
            return pieceQueue.take();
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
        private PieceSpecSetIface pieceSpecSet = EmptyPieceSpecSet.INSTANCE;

        /**
         * Adds a data-specification.
         * 
         * @param spec
         *            The specification of the data to be added.
         */
        synchronized void put(final FilePieceSpecSet spec) {
            pieceSpecSet = pieceSpecSet.merge(spec);
            notify();
        }

        /**
         * Adds a set of data-piece specifications.
         * 
         * @param specs
         *            The set of data-piece specifications. The client must not
         *            subsequently modify this set.
         */
        synchronized void put(final PieceSpecSetIface specs) {
            pieceSpecSet = pieceSpecSet.merge(specs);
            notify();
        }

        /**
         * Indicates if this instance has a specification or not.
         * 
         * @return {@code true} if and only if this instance has a
         *         specification.
         */
        synchronized boolean isEmpty() {
            return pieceSpecSet.isEmpty();
        }

        /**
         * Returns the next data-specification. Blocks until one is available.
         * 
         * @return The next data-specification.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         */
        synchronized PieceSpecSetIface take() throws InterruptedException {
            while (isEmpty()) {
                wait();
            }
            return removeAndReturn();
        }

        /**
         * Removes and returns the next data-specification if one exists;
         * otherwise, returns {@code null}.
         * 
         * @return The next data-specification of one exists; otherwise,
         *         {@code null}.
         */
        synchronized PieceSpecSetIface poll() {
            return isEmpty()
                    ? null
                    : removeAndReturn();
        }

        @GuardedBy("this")
        synchronized private PieceSpecSetIface removeAndReturn() {
            final PieceSpecSetIface specs = pieceSpecSet;
            pieceSpecSet = EmptyPieceSpecSet.INSTANCE;
            notify();
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
        private ArchivePathSet      removals    = new ArchivePathSet();

        /**
         * Adds a notice about new data.
         * 
         * @param spec
         *            The specification of the new data.
         */
        synchronized void newData(final FilePieceSpecSet spec) {
            /*
             * NB: Notices about new data accumulate unconditionally.
             */
            additions.put(spec);
            logger.trace("New-data notice added: {}", spec);
            notify();
        }

        /**
         * Adds a notice of old data.
         * <p>
         * This is a potentially lengthy operation because a notice about old
         * data won't be added as long as a notice about new data remains to be
         * sent.
         * 
         * @param spec
         *            The specification of the data to be added.
         * @throws InterruptedException
         *             if the current thread is interrupted
         */
        synchronized void oldData(final FilePieceSpecSet spec)
                throws InterruptedException {
            /*
             * NB: Notices about old data do not accumulate unconditionally in
             * order to favor the transmission of new data over old data.
             */
            while (!additions.isEmpty()) {
                wait();
            }
            additions.put(spec);
            logger.trace("Old-data notice added: {}", spec);
            notify();
        }

        /**
         * Adds a notice of removal of a file.
         * 
         * @param archivePath
         *            The archive-pathname of the file.
         */
        synchronized void put(final ArchivePath archivePath) {
            removals.add(archivePath);
            logger.trace("Removal notice added: {}", archivePath);
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
                removals = new ArchivePathSet();
                wasAddition = false;
            }
            else {
                notice = new AdditionNotice(additions.poll());
                wasAddition = true;
            }
            notify();
            return notice;
        }
    }
}
