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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

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
    private static final Logger               logger               = Logger
                                                                           .getLogger(Peer.class
                                                                                   .getName());
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
    private final DataSpecQueue               noticeQueue          = new DataSpecQueue();
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
        if (remotePredicate.satisfiedBy(pieceSpec.getFileInfo())) {
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
    void newData(final PiecesSpec spec) {
        if (remotePredicate.satisfiedBy(spec.getFileInfo())) {
            noticeQueue.put(spec);
        }
    }

    /**
     * Sets the name of the current thread.
     */
    private static void setThreadName(final String className) {
        final Thread currentThread = Thread.currentThread();
        currentThread.setName(className);
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
            clearingHouse.walkArchive(new FileSpecConsumer() {
                @Override
                public void consume(final PiecesSpec spec) {
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
    private static abstract class Receiver<T> extends SocketUsingTask<Void> {
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
         * @param socket
         *            The socket.
         * @param type
         *            The type of the received objects.
         * @throws IOException
         *             if an input stream can't be gotten from the socket.
         * @throws NullPointerException
         *             if {socket == null || type == null}.
         */
        protected Receiver(final Socket socket, final Class<T> type)
                throws IOException {
            super(socket);
            if (null == type) {
                throw new NullPointerException();
            }

            this.inputStream = socket.getInputStream();
            this.type = type;
        }

        /**
         * Reads objects from the connection with the remote peer and processes
         * them. Will block until the connection is initialized by the remote
         * peer. Completes normally if and only if 1) an end-of-file is read; or
         * 2) the subclass indicates that no more processing should occur.
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
                    final Object obj = objectInputStream.readUnshared();
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
            catch (final InterruptedException e) {
                // Implements thread interruption policy
            }
            catch (final IOException e) {
                if (!Thread.currentThread().isInterrupted()) {
                    throw e;
                }
            }
            return null;
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
    }

    /**
     * Receives requests for data and processes them.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private final class RequestReceiver extends Receiver<SpecThing> implements
            SpecProcessor {
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
            super(connection.getRequestSocket(), SpecThing.class);
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
            final Object obj = ois.readObject();
            logger.finest("Received predicate: " + obj);
            remotePredicateQueue.put((Predicate) obj);
        }

        @Override
        protected boolean process(final SpecThing request)
                throws InterruptedException, IOException {
            logger.finest("Received request: " + request);
            request.process(this);
            return true;
        }

        @Override
        public void process(final PieceSpec pieceSpec)
                throws InterruptedException, IOException {
            pieceQueue.put(clearingHouse.getPiece(pieceSpec));
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
    private final class NoticeReceiver extends Receiver<SpecThing> implements
            SpecProcessor {
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
            super(connection.getNoticeSocket(), SpecThing.class);
        }

        @Override
        protected boolean process(final SpecThing notice) throws IOException,
                InterruptedException {
            logger.finest("Received notice: " + notice);
            notice.process(this);
            return true;
        }

        @Override
        public void process(final PieceSpec pieceSpec) throws IOException {
            clearingHouse.process(Peer.this, pieceSpec);
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
            logger.finest("Received piece: " + piece);
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
    private static abstract class Sender<T> extends SocketUsingTask<Void> {
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
            super(socket);
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
                    objectOutputStream.writeObject(obj);
                    objectOutputStream.flush();
                    objectOutputStream.reset();
                }
            }
            catch (final InterruptedException ignored) {
                // Implements thread interruption policy
            }
            catch (final IOException e) {
                if (!Thread.currentThread().isInterrupted()) {
                    throw e;
                }
            }
            return null;
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
    private final class RequestSender extends Sender<SpecThing> {
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
            logger.finest("Sending predicate: " + localPredicate);
            oos.writeObject(localPredicate);
            oos.flush();
        }

        @Override
        public SpecThing nextObject() throws InterruptedException {
            final SpecThing request = requestQueue.take();
            logger.finest("Sending request: " + request);
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
    private final class NoticeSender extends Sender<SpecThing> {
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
        protected SpecThing nextObject() throws InterruptedException {
            final SpecThing notice = noticeQueue.take();
            logger.finest("Sending notice: " + notice);
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
            logger.finest("Sending piece: " + piece);
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
            if (callable instanceof SocketUsingTask<?>) {
                return ((SocketUsingTask<T>) callable).newTask();
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
    private static abstract class SocketUsingTask<T> implements Callable<T> {
        private final Socket socket;

        SocketUsingTask(final Socket socket) {
            if (null == socket) {
                throw new NullPointerException();
            }
            this.socket = socket;
        }

        RunnableFuture<T> newTask() {
            return new FutureTask<T>(this) {
                @SuppressWarnings("finally")
                @Override
                public boolean cancel(final boolean mayInterruptIfRunning) {
                    try {
                        socket.close();
                    }
                    catch (final IOException ignored) {
                    }
                    finally {
                        return super.cancel(mayInterruptIfRunning);
                    }
                }
            };
        }
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
         * The map from file information to data-specification.
         */
        @GuardedBy("this")
        private final Map<FileInfo, PiecesSpec> piecesSpecs = new HashMap<FileInfo, PiecesSpec>();

        /**
         * Adds a data-specification.
         * 
         * @param spec
         *            The specification of the data to be added.
         */
        synchronized void put(final PiecesSpec spec) {
            final FileInfo fileInfo = spec.getFileInfo();
            PiecesSpec entry = piecesSpecs.remove(fileInfo);
            entry = (null == entry)
                    ? spec
                    : entry.merge(spec);
            piecesSpecs.put(fileInfo, entry);
            notify();
        }

        /**
         * Returns the next data-specification. Blocks until one is available.
         * 
         * @return The next data-specification.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         */
        synchronized SpecThing take() throws InterruptedException {
            while (piecesSpecs.isEmpty()) {
                wait();
            }
            final SpecThing specThing = SpecThing.newInstance(piecesSpecs
                    .values());
            piecesSpecs.clear();
            return specThing;
        }
    }
}