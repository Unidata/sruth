/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Communicates with its remote counterpart.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class Peer implements Callable<Void> {
    /**
     * The logging service.
     */
    private static final Logger               logger               = LoggerFactory
                                                                           .getLogger(Peer.class);
    /**
     * The executor service for tasks.
     */
    private static final ExecutorService      executorService      = new CancellingExecutor();
    /**
     * The task manager.
     */
    private final TaskManager<Void>           taskManager          = new TaskManager<Void>(
                                                                           executorService);
    /**
     * The data clearing-house to use
     */
    private final ClearingHouse               clearingHouse;
    /**
     * The connection with the remote peer.
     */
    private final Connection                  connection;
    /**
     * Rendezvous-queue for the remotely-desired data.
     */
    private final SynchronousQueue<Predicate> remotePredicateQueue = new SynchronousQueue<Predicate>();
    /**
     * Notice queue. Contains notices of data to be sent to the remote peer.
     */
    private final NoticeQueue                 noticeQueue          = new NoticeQueue();
    /**
     * Piece queue. Contains pieces of data to be sent to the remote peer.
     */
    private final BlockingQueue<Piece>        pieceQueue           = new SynchronousQueue<Piece>();
    /**
     * Specification of data desired by the local peer.
     */
    private final Predicate                   localPredicate;
    /**
     * Specification of data desired by the remote peer.
     */
    private volatile Predicate                remotePredicate;
    /**
     * Request queue. Contains specifications of data-pieces to request from the
     * remote peer.
     */
    private final DataSpecQueue               requestQueue         = new DataSpecQueue();

    /**
     * Constructs from the pathname of the root of the file-tree and a
     * connection to a remote peer.
     * 
     * @param clearingHouse
     *            The data clearing-house to use.
     * @param connection
     *            The connection to the remote peer.
     * @throws NullPointerException
     *             if {@code clearingHouse == null || connection == null}.
     */
    Peer(final ClearingHouse clearingHouse, final Connection connection) {
        if (null == connection) {
            throw new NullPointerException();
        }

        this.clearingHouse = clearingHouse;
        this.connection = connection;
        localPredicate = clearingHouse.getPredicate();
        clearingHouse.add(this);
    }

    /**
     * Executes this instance and completes normally if and only if 1) all tasks
     * terminate normally; or 2) the current thread is interrupted. Upon
     * completion, the connection to the remote peer is closed.
     * 
     * @throws ExecutionException
     *             if an exception was thrown.
     * @throws IOException
     *             if an I/O error occurs.
     */
    @Override
    public final Void call() throws ExecutionException, IOException {
        try {
            taskManager.submit(new RequestReceiver(connection));
            taskManager.submit(new RequestSender(connection));

            remotePredicate = remotePredicateQueue.take();

            taskManager.submit(new NoticeSender(connection));
            taskManager.submit(new PieceSender(connection));

            if (!Predicate.NOTHING.equals(localPredicate)) {
                // This peer wants data
                taskManager.submit(new NoticeReceiver(connection));
                taskManager.submit(new PieceReceiver(connection));
            }

            // Check the archive for remotely-desired data
            taskManager.submit(new FileScanner());

            taskManager.waitUpon();
        }
        catch (final InterruptedException ignored) {
            // Implements interruption policy of thread
        }
        finally {
            taskManager.cancel();
            connection.close();
        }

        return null;
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
        if (remotePredicate.satisfiedBy(pieceSpec)) {
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
     * Handles the creation of a new local data by notifying the remote peer of
     * the newly-available data.
     * 
     * @param spec
     *            Specification of the new data.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void newData(final FilePieceSpecSet spec) {
        if (remotePredicate.satisfiedBy(spec.getFileInfo())) {
            noticeQueue.put(spec);
        }
    }

    /**
     * Responds to the removal of a local file or category by notifying the
     * remote peer.
     * 
     * @param fileId
     *            Identifier of the file or category.
     */
    void notifyRemoteOfRemovals(final FileId fileId) {
        noticeQueue.put(fileId);
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
        clearingHouse.process(this, pieceSpec);
    }

    /**
     * Scans the archive for remotely-desired data and adds it to the notice
     * queue.
     * 
     * @author Steven E. Emmerson
     */
    @ThreadSafe
    private final class FileScanner implements Callable<Void> {
        public Void call() throws InterruptedException {
            clearingHouse.walkArchive(new PiecesSpecConsumer() {
                @Override
                public void consume(final FilePieceSpecSet spec) {
                    noticeQueue.put(spec);
                }
            }, remotePredicate);
            return null;
        }
    }

    /**
     * Receives objects from the remote peer and acts upon them.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private static abstract class Receiver<T> extends StreamUsingTask<Void> {
        /**
         * The input stream from the remote peer.
         */
        private final InputStream inputStream;
        /**
         * The type of the received objects.
         */
        private final Class<T>    type;

        /**
         * Constructs from a socket connection with the remote peer.
         * 
         * @param socket
         *            The socket connection with the remote peer.
         * @param type
         *            The type of the received objects.
         * @throws IOException
         *             if an input stream can't be gotten from the socket.
         * @throws NullPointerException
         *             if {socket == null || type == null}.
         */
        protected Receiver(final Socket socket, final Class<T> type)
                throws IOException {
            if (null == type) {
                throw new NullPointerException();
            }

            this.inputStream = socket.getInputStream();
            this.type = type;
        }

        /**
         * Reads objects from the connection with the remote peer and processes
         * them. Will block until the connection is initialized by the remote
         * peer. Completes normally if and only if 1) the connection is closed;
         * or 2) the subclass indicates that no more processing should occur
         * (because all the desired data has been received, for example).
         * 
         * @throws ClassCastException
         *             if the object has the wrong type.
         * @throws ClassNotFoundException
         *             if object has unknown type.
         * @throws IOException
         *             if an I/O error occurs.
         */
        public Void call() throws IOException, ClassNotFoundException {
            setThreadName(getClass().getSimpleName());
            final ObjectInputStream objectInputStream = new ObjectInputStream(
                    inputStream);
            try {
                initialize(objectInputStream);
                for (;;) {
                    final Object obj = readObject(objectInputStream);
                    if (null == obj) {
                        break;
                    }
                    if (!process(type.cast(obj))) {
                        break;
                    }
                }
            }
            catch (final EOFException e) {
                // ignored
            }
            catch (final InterruptedException ignored) {
                // Implements thread interruption policy
            }
            catch (final IOException e) {
                if (!isCancelled) {
                    throw e;
                }
            }
            return null;
        }

        /**
         * Returns the next object from an object input stream or {@code null}
         * if no more objects are forthcoming. This method should be used for
         * receiving all objects to the remote peer -- including any received by
         * {@link #initialize(ObjectOutputStream)}.
         * 
         * @param ois
         *            The object input stream.
         * @return The next object from the object input stream or {@code null}.
         * @throws ClassCastException
         *             if the object has the wrong type.
         * @throws ClassNotFoundException
         *             if object has unknown type.
         * @throws IOException
         *             if an I/O error occurs.
         */
        protected final Object readObject(final ObjectInputStream ois)
                throws IOException, ClassNotFoundException {
            try {
                final Object obj = ois.readUnshared();
                logger.trace("Received {}", obj);
                return obj;
            }
            catch (final SocketException ignored) {
                /*
                 * The possible subclasses of the exception are: BindException,
                 * ConnectException, NoRouteToHostException, and
                 * PortUnreachableException -- none of which can occur here
                 * because the connection is established. Consequently, the
                 * exception is most likely due to the connection being reset
                 * (i.e., closed) by the remote peer.
                 */
                return null;
            }
        }

        /**
         * Performs any necessary initialization of the object input stream.
         * 
         * @param ois
         *            The object input stream.
         * @throws ClassNotFoundException
         *             if a read object has the wrong type.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         * @throws IOException
         *             if an I/O error occurs.
         */
        protected void initialize(final ObjectInputStream ois)
                throws IOException, InterruptedException,
                ClassNotFoundException {
            // The default does nothing
        }

        /**
         * Processes an object.
         * 
         * @param obj
         *            The object to process.
         * @return {@code true} if and only if processing should continue.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         * @throws IOException
         *             if an I/O error occurs.
         */
        protected abstract boolean process(T obj) throws IOException,
                InterruptedException;

        /**
         * Closes the input stream.
         */
        @Override
        protected final void close() {
            try {
                inputStream.close();
            }
            catch (final IOException ignored) {
            }
        }
    }

    /**
     * Receives requests for data and processes them.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private final class RequestReceiver extends Receiver<PieceSpecSet> {
        /**
         * Constructs from a connection to the remote peer. Will block until the
         * remote peer initializes the relevant object stream.
         * 
         * @param connection
         *            The connection to the remote peer.
         * @throws IOException
         *             if an input stream can't be gotten from the connection.
         * @throws NullPointerException
         *             if {connection == null}.
         */
        RequestReceiver(final Connection connection) throws IOException {
            super(connection.getRequestSocket(), PieceSpecSet.class);
        }

        /**
         * Reads the specification of remotely-desired data from the object
         * input stream and puts it in the remote predicate queue.
         * 
         * @param ois
         *            The object input stream.
         * @throws ClassNotFoundException
         *             if the read object has the wrong type.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         * @throws IOException
         *             if an I/O error occurs.
         */
        @Override
        protected void initialize(final ObjectInputStream ois)
                throws IOException, InterruptedException,
                ClassNotFoundException {
            final Object obj = readObject(ois);
            remotePredicateQueue.put((Predicate) obj);
        }

        @Override
        protected boolean process(final PieceSpecSet request)
                throws InterruptedException, IOException {
            for (final PieceSpec spec : request) {
                pieceQueue.put(clearingHouse.getPiece(spec));
            }
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
    @ThreadSafe
    private final class NoticeReceiver extends Receiver<Notice> {
        /**
         * Constructs from a connection to the remote peer.
         * 
         * @param connection
         *            The connection to the remote peer.
         * @throws IOException
         *             if an input stream can't be gotten from the connection.
         * @throws NullPointerException
         *             if {connection == null}.
         */
        NoticeReceiver(final Connection connection) throws IOException {
            super(connection.getNoticeSocket(), Notice.class);
        }

        @Override
        protected boolean process(final Notice notice) throws IOException,
                InterruptedException {
            notice.processYourself(Peer.this);
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
    @ThreadSafe
    private final class PieceReceiver extends Receiver<Piece> {
        /**
         * Constructs from a connection to the remote peer.
         * 
         * @param connection
         *            The connection to the remote peer.
         * @throws IOException
         *             if an input stream can't be gotten from the connection.
         * @throws NullPointerException
         *             if {connection == null}.
         */
        PieceReceiver(final Connection connection) throws IOException {
            super(connection.getDataSocket(), Piece.class);
        }

        @Override
        protected boolean process(final Piece piece) throws IOException,
                InterruptedException {
            final boolean keepGoing = !clearingHouse.process(Peer.this, piece);
            if (!keepGoing) {
                taskManager.cancel();
            }
            return keepGoing;
        }
    }

    /**
     * Sends objects to the remote peer.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private static abstract class Sender<T> extends StreamUsingTask<Void> {
        /**
         * The output stream to the remote peer.
         */
        private final OutputStream outputStream;

        /**
         * Constructs from a local peer and the output stream to the remote
         * peer.
         * 
         * @param socket
         *            The relevant socket.
         * @throws IOException
         *             if an output stream can't be gotten from the socket.
         * @throws NullPointerException
         *             if {socket == null}.
         */
        protected Sender(final Socket socket) throws IOException {
            this.outputStream = socket.getOutputStream();
        }

        /**
         * Executes this instance. Completes normally if and only if 1) the
         * {@link #nextObject} method returns {@code null}; or 2) the current
         * thread is interrupted.
         * 
         * @throws IOException
         *             if an I/O error occurs.
         */
        @Override
        public final Void call() throws IOException {
            setThreadName(getClass().getSimpleName());
            final ObjectOutputStream objectOutputStream = new ObjectOutputStream(
                    outputStream);
            initialize(objectOutputStream);
            try {
                for (T obj = nextObject(); null != obj; obj = nextObject()) {
                    writeObject(obj, objectOutputStream);
                }
            }
            catch (final InterruptedException ignored) {
                // Implements thread interruption policy
            }
            catch (final IOException e) {
                if (!isCancelled) {
                    throw e;
                }
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
        protected final void writeObject(final Object obj,
                final ObjectOutputStream oos) throws IOException {
            logger.trace("Sending {}", obj);
            oos.writeObject(obj);
            oos.flush();
            oos.reset();
        }

        /**
         * Performs any necessary initialization of the object output stream.
         * 
         * @param oos
         *            The object output stream.
         * @throws IOException
         *             if an I/O error occurs.
         */
        protected void initialize(final ObjectOutputStream oos)
                throws IOException {
            // Does nothing by default
        }

        /**
         * Returns the next object to send.
         * 
         * @return The next object to send.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         */
        protected abstract T nextObject() throws InterruptedException;

        /**
         * Closes the output stream.
         */
        @Override
        protected final void close() {
            try {
                outputStream.close();
            }
            catch (final IOException ignored) {
            }
        }
    }

    /**
     * Sends requests for data to the remote peer after initially sending the
     * specification of locally-desired data.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private final class RequestSender extends Sender<PieceSpecSet> {
        /**
         * Constructs from a peer and the connection to the remote peer.
         * 
         * @param connection
         *            The connection to the remote peer.
         * @throws IOException
         *             if an output stream can't be gotten from the connection.
         * @throws NullPointerException
         *             if {connection == null}.
         */
        RequestSender(final Connection connection) throws IOException {
            super(connection.getRequestSocket());
        }

        @Override
        protected void initialize(final ObjectOutputStream oos)
                throws IOException {
            writeObject(localPredicate, oos);
        }

        @Override
        public PieceSpecSet nextObject() throws InterruptedException {
            final PieceSpecSet request = requestQueue.take();
            return request;
        }
    }

    /**
     * Sends notices of available data to a remote peer.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private final class NoticeSender extends Sender<Notice> {
        /**
         * Constructs from the connection to a remote peer and a specification
         * of the data desired by the remote peer.
         * 
         * @param Connection
         *            The connection to the remote peer.
         * @throws IOException
         *             if an output stream can't be gotten from the connection.
         * @throws NullPointerException
         *             if {connection == null}.
         */
        NoticeSender(final Connection connection) throws IOException {
            super(connection.getNoticeSocket());
        }

        @Override
        protected Notice nextObject() throws InterruptedException {
            final Notice notice = noticeQueue.take();
            return notice;
        }
    }

    /**
     * Sends pieces of data to the remote peer.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private final class PieceSender extends Sender<Piece> {
        /**
         * Constructs from a connection to the remote peer.
         * 
         * @param Connection
         *            The connection to the remote peer.
         * @throws IOException
         *             if an output stream can't be gotten from the connection.
         * @throws NullPointerException
         *             if {connection == null}.
         */
        PieceSender(final Connection connection) throws IOException {
            super(connection.getDataSocket());
        }

        @Override
        protected Piece nextObject() throws InterruptedException {
            final Piece piece = pieceQueue.take();
            return piece;
        }
    }

    /**
     * An executor service that supports the cancellation of tasks that use
     * sockets.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private static class CancellingExecutor extends ThreadPoolExecutor {
        public CancellingExecutor() {
            super(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
                    new SynchronousQueue<Runnable>());
        }

        @Override
        protected <T> RunnableFuture<T> newTaskFor(final Callable<T> callable) {
            if (callable instanceof StreamUsingTask<?>) {
                return ((StreamUsingTask<T>) callable).newTask();
            }
            else {
                return super.newTaskFor(callable);
            }
        }
    }

    /**
     * A task that uses a socket and that returns a {@link RunnableFuture} whose
     * {@link Future#cancel()} method closes the socket.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private static abstract class StreamUsingTask<T> implements Callable<T> {
        protected volatile boolean isCancelled;

        RunnableFuture<T> newTask() {
            return new FutureTask<T>(this) {
                @Override
                public boolean cancel(final boolean mayInterruptIfRunning) {
                    isCancelled = true;
                    close();
                    return super.cancel(mayInterruptIfRunning);
                }
            };
        }

        /**
         * Closes the stream.
         */
        protected abstract void close();
    }

    /**
     * A queue of data-specifications.
     * 
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
         * @return The next data-specification of one exists; otherwise, {@code
         *         null}.
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
     * 
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
         * Adds a notice of removal of a file or category.
         * 
         * @param id
         *            The identifier of the file or category.
         */
        synchronized void put(final FileId id) {
            removals.add(id);
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