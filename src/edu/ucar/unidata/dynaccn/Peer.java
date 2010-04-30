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
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

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
     * The clearing-house used by this instance.
     */
    private final ClearingHouse                   clearingHouse;
    /**
     * The connection with the remote peer.
     */
    private final Connection                      connection;
    /**
     * Whether or not this instance is done.
     */
    private final AtomicBoolean                   done              = new AtomicBoolean(
                                                                            false);
    /**
     * Whether or not this instance wants some data.
     */
    private final boolean                         wantsData;

    /**
     * Constructs from a clearing-house to use and a connection to a remote
     * peer.
     * 
     * @param clearingHouse
     *            The clearing-house to use.
     * @param connection
     *            The connection to the remote peer.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code clearingHouse == null || connection == null}.
     */
    Peer(final ClearingHouse clearingHouse, final Connection connection)
            throws IOException {
        if (null == clearingHouse || null == connection) {
            throw new NullPointerException();
        }

        this.clearingHouse = clearingHouse;
        this.connection = connection;
        wantsData = !Predicate.NOTHING.equals(clearingHouse.getPredicate());
        clearingHouse.addPeer(this);
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
            completionService.submit(new RequestSender(connection));
            completionService.submit(new RequestReceiver(connection));

            clearingHouse.waitForRemotePredicate(Peer.this);

            completionService.submit(new NoticeSender(connection));
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
     * Processes a notice of a file available at the remote peer.
     * 
     * @param fileNotice
     *            The notice of an available file.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void process(final FileNotice fileNotice) throws IOException {
        clearingHouse.process(fileNotice);
    }

    /**
     * Processes a notice of an available piece of data at the remote peer.
     * 
     * @param pieceNotice
     *            Notice of the available piece of data.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    void process(final PieceNotice pieceNotice) throws InterruptedException {
        clearingHouse.process(Peer.this, pieceNotice);
    }

    /**
     * Processes a specification of data desired by the remote peer.
     * 
     * @param predicateRequest
     *            The specification of remotely-desired data.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    void process(final PredicateRequest predicateRequest)
            throws InterruptedException {
        clearingHouse.process(Peer.this, predicateRequest);
    }

    /**
     * Processes a request for a piece of data.
     * 
     * @param pieceRequest
     *            The request for a piece of data.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    void process(final PieceRequest pieceRequest) throws InterruptedException,
            IOException {
        clearingHouse.process(Peer.this, pieceRequest);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((connection == null)
                ? 0
                : connection.hashCode());
        return result;
    }

    /**
     * Indicates if this instance is considered equal to an object. Two
     * instances are considered equal if and only if their connections to the
     * remote peer are equal.
     * 
     * @see Connection#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Peer other = (Peer) obj;
        if (connection == null) {
            if (other.connection != null) {
                return false;
            }
        }
        else if (!connection.equals(other.connection)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{connection=" + connection + "}";
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
                    System.out.println("Received: " + obj);
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
        protected boolean process(final Piece piece) throws IOException,
                InterruptedException {
            final boolean keepGoing = !(clearingHouse.process(Peer.this, piece) && wantsData);
            if (!keepGoing) {
                done.set(true);
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
            for (T obj = nextObject(); null != obj; obj = nextObject()) {
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
            return clearingHouse.getNextRequest(Peer.this);
        }
    }

    /**
     * Sends notices of available data to a remote peer.
     * 
     * Instances are thread-compatible but not thread-safe (due to the use of
     * the peer's {@link NoticeIterator}).
     * 
     * @author Steven R. Emmerson
     */
    private final class NoticeSender extends Sender<Notice> {
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
        NoticeSender(final Connection connection) throws IOException {
            super(connection.getNoticeOutputStream());
        }

        @Override
        protected Notice nextObject() throws InterruptedException {
            return clearingHouse.getNextNotice(Peer.this);
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
            return clearingHouse.getNextPiece(Peer.this);
        }
    }
}
