package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.prefs.Preferences;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.Immutable;
import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;

/**
 * A high-level connection to a remote end-point comprising multiple,
 * bi-directional streams.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
abstract class Connection implements Comparable<Connection> {
    /**
     * Uniquely identifies a {@link Connection} within the JVM.
     * <p>
     * Instances are immutable.
     * 
     * @author Steven R. Emmerson
     */
    @Immutable
    static final class ConnectionId implements Message {
        /**
         * The serial version identifier.
         */
        private static final long       serialVersionUID  = 1L;
        /**
         * The node identifier.
         * 
         * @serial
         */
        private final InetSocketAddress serverSocketAddress;
        /**
         * The connection index within the node.
         * 
         * @serial
         */
        private final long              index;
        /**
         * The connection counter.
         */
        private static final AtomicLong connectionCounter = new AtomicLong(0);

        /**
         * Constructs from the address of a server.
         * 
         * @param serverSocketAddress
         *            The address of a server.
         * @throws NullPointerException
         *             if {@code serverSocketAddress == null}.
         */
        ConnectionId(final InetSocketAddress serverSocketAddress) {
            // TODO: Get the connection ID from the server
            this.serverSocketAddress = serverSocketAddress;
            index = connectionCounter.getAndIncrement();
        }

        /**
         * Returns the address of the server.
         * 
         * @return the address of the server.
         */
        InetSocketAddress getServerAddress() {
            return serverSocketAddress;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            return serverSocketAddress.hashCode()
                    ^ Long.valueOf(index).hashCode();
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#equals(java.lang.Object)
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
            final ConnectionId other = (ConnectionId) obj;
            if (serverSocketAddress == null) {
                if (other.serverSocketAddress != null) {
                    return false;
                }
            }
            else if (!serverSocketAddress.equals(other.serverSocketAddress)) {
                return false;
            }
            else if (index != other.index) {
                return false;
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
            return "ConnectionId [serverSocketAddress=" + serverSocketAddress
                    + ", index=" + index + "]";
        }
    }

    /**
     * Interface for objects that can be sent across a {@link Connection}.
     * 
     * @author Steven R. Emmerson
     */
    interface Message extends Serializable {
    }

    /**
     * A single, bi-directional, communications channel.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    protected abstract static class Stream implements Comparable<Stream> {
        /**
         * A single, write-only communications channel.
         * <p>
         * Instances are thread-safe.
         * 
         * @author Steven R. Emmerson
         */
        protected final class Output {
            /**
             * The underlying object output stream.
             */
            private final ObjectOutputStream objectOutputStream;

            /**
             * Constructs from an object output stream.
             * 
             * @param output
             *            The object output stream.
             * @throws NullPointerException
             *             if {@code output == null}.
             */
            Output(final ObjectOutputStream objectOutputStream) {
                if (objectOutputStream == null) {
                    throw new NullPointerException();
                }
                this.objectOutputStream = objectOutputStream;
            }

            /**
             * Sends an object on the stream.
             * <p>
             * This is an uninterruptible and potentially lengthy operation.
             * 
             * @param obj
             *            The object to be sent.
             * @throws IOException
             *             if an I/O error occurs.
             */
            void send(final Message obj) throws IOException {
                logger.trace("Sending {}", obj);
                objectOutputStream.writeObject(obj);
                objectOutputStream.reset();
                objectOutputStream.flush();
            }

            /**
             * Closes this instance. Idempotent.
             */
            synchronized void close() {
                if (!socket.isClosed() && !socket.isOutputShutdown()) {
                    try {
                        socket.shutdownOutput();
                    }
                    catch (final IOException e) {
                        logger.debug("Couldn't close socket output: {}",
                                e.toString());
                    }
                }
                /*
                 * The following is commented-out because it causes the
                 * associated input stream to return EOF.
                 */
                // try {
                // objectOutputStream.close();
                // }
                // catch (final IOException ignored) {
                // }
                // finally {
                // try {
                // socket.shutdownOutput();
                // }
                // catch (final IOException ignored) {
                // }
                // }
            }
        }

        /**
         * A single, read-only communications channel.
         * <p>
         * Instances are thread-safe.
         * 
         * @author Steven R. Emmerson
         */
        protected final class Input {
            /**
             * The underlying object input stream.
             */
            private final ObjectInputStream objectInputStream;

            /**
             * Constructs from an object input stream.
             * 
             * @param input
             *            The object input stream.
             * @throws NullPointerException
             *             if {@code input == null}.
             */
            Input(final ObjectInputStream objectInputStream) {
                if (objectInputStream == null) {
                    throw new NullPointerException();
                }
                this.objectInputStream = objectInputStream;
            }

            /**
             * Receives an object from the stream within a given time interval.
             * <p>
             * This is an uninterruptible and potentially lengthy operation.
             * 
             * @param timeout
             *            The timeout interval in milliseconds. A value of
             *            {@code 0} disables the timeout mechanism.
             * @return The object received from the stream.
             * @throws ClassNotFoundException
             *             if the type of the received object isn't known.
             * @throws IllegalArgumentException
             *             if {@code timeout < 0}.
             * @throws IOException
             *             if an I/O error occurs.
             * @throws SocketException
             *             if the connection was closed by the remote peer.
             * @throws SocketTimeoutException
             *             if the timeout expires. The underlying stream is
             *             still valid.
             */
            Object receiveObject(final int timeout)
                    throws ClassNotFoundException, IOException,
                    SocketException, SocketTimeoutException {
                if (timeout < 0) {
                    throw new IllegalArgumentException();
                }
                socket.setSoTimeout(timeout);
                final Object obj = objectInputStream.readUnshared();
                logger.trace("Received {}", obj);
                return obj;
            }

            /**
             * Closes this instance. Idempotent.
             */
            synchronized void close() {
                if (!socket.isClosed() && !socket.isInputShutdown()) {
                    try {
                        socket.shutdownInput();
                    }
                    catch (final IOException e) {
                        logger.debug("Couldn't close socket input: {}",
                                e.toString());
                    }
                }
                /*
                 * The following is commented-out because it causes the
                 * associated output stream to throw an IOException.
                 */
                // try {
                // objectInputStream.close();
                // }
                // catch (final IOException ignored) {
                // }
                // finally {
                // try {
                // socket.shutdownInput();
                // }
                // catch (final IOException ignored) {
                // }
                // }
            }
        }

        /**
         * The underlying socket.
         */
        private final Socket        socket;
        /**
         * The address of the client's socket.
         */
        @GuardedBy("this")
        protected InetSocketAddress clientSocketAddress;
        /**
         * The input stream.
         */
        @GuardedBy("this")
        protected Stream.Input      input;
        /**
         * The output stream.
         */
        @GuardedBy("this")
        protected Stream.Output     output;
        /**
         * The address of the server's socket at the remote host.
         */
        @GuardedBy("this")
        protected InetSocketAddress remoteServerSocketAddress;

        /**
         * Constructs from a socket.
         * 
         * @param socket
         *            The socket.
         * @throws IOException
         *             if an I/O error occurs.
         * @throws NullPointerException
         *             if {@code socket == null}.
         */
        Stream(final Socket socket) throws IOException {
            if (socket == null) {
                throw new NullPointerException();
            }
            this.socket = socket;
        }

        /**
         * Returns the underlying socket.
         */
        Socket getSocket() {
            return socket;
        }

        /**
         * Returns the remote Internet address to which the underlying socket is
         * connected.
         * 
         * @return The remote Internet address to which the underlying socket is
         *         connected.
         */
        InetAddress getInetAddress() {
            return socket.getInetAddress();
        }

        /**
         * Returns the client-side port number.
         * 
         * @return The client-side port number.
         */
        @GuardedBy("this")
        synchronized int getClientPort() {
            return clientSocketAddress.getPort();
        }

        /**
         * Returns the output stream.
         * 
         * @return The output stream.
         */
        @GuardedBy("this")
        synchronized Output getOutput() {
            return output;
        }

        /**
         * Returns the input stream.
         * 
         * @return The input stream.
         */
        @GuardedBy("this")
        synchronized Input getInput() {
            return input;
        }

        /**
         * Sends an object on the stream. This is equivalent to
         * {@link Output#send(Message) getOutput().send(Object)}.
         * <p>
         * This is an uninterruptible and potentially lengthy operation.
         * 
         * @param obj
         *            The object to be sent.
         * @throws IOException
         *             if an I/O error occurs.
         */
        void send(final Message obj) throws IOException {
            getOutput().send(obj);
        }

        /**
         * Receives an object from the stream. This is equivalent to
         * {@link Input#receiveObject(int) receiveObject(0)}.
         * <p>
         * This is an uninterruptible and potentially lengthy operation.
         * 
         * @return The object received from the stream.
         * @throws ClassNotFoundException
         *             if the type of the received object isn't known.
         * @throws IOException
         *             if an I/O error occurs.
         */
        Object receiveObject() throws IOException, ClassNotFoundException {
            return receiveObject(0);
        }

        /**
         * Receives an object from the stream within a specified timeout. This
         * is equivalent to {@link Input#receiveObject(int)
         * getInput().receiveObject(timeout)}.
         * <p>
         * This is an uninterruptible and potentially lengthy operation.
         * 
         * @param timeout
         *            The timeout interval in milliseconds. A value of {@code 0}
         *            disables the timeout mechanism.
         * @return The object received from the stream.
         * @throws ClassNotFoundException
         *             if the type of the received object isn't known.
         * @throws IllegalArgumentException
         *             if {@code timeout < 0}.
         * @throws IOException
         *             if an I/O error occurs.
         * @throws SocketTimeoutException
         *             if the timeout expires. The underlying stream is still
         *             valid.
         */
        Object receiveObject(final int timeout) throws IOException,
                ClassNotFoundException {
            return getInput().receiveObject(timeout);
        }

        /**
         * Closes this instance, releasing all resources. NB: Closes the
         * underlying socket. Idempotent.
         */
        @GuardedBy("this")
        synchronized void close() {
            input.close();
            output.close();
            try {
                socket.close();
            }
            catch (final IOException ignored) {
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return getClass().getSimpleName() + " [socket=" + socket + "]";
        }

        /**
         * Compares this instance with another. Streams are sorted first by the
         * socket address of the socket address of the server at the remote host
         * and then by their client-side socket addresses. For streams of the
         * same client, this means that they'll be sorted by their port numbers
         * at both ends of the connection.
         * 
         * @param that
         *            The other instance.
         * @return A value less than, equal to, or greater than zero as this
         *         instance is considered less than, equal to, or greater than
         *         the other instance, respectively.
         * @throws NullPointerException
         *             if {@code that == null}.
         */
        public int compareTo(final Stream that) {
            if (this == that) {
                return 0;
            }
            Stream o1, o2;
            if (System.identityHashCode(this) < System.identityHashCode(that)) {
                o1 = this;
                o2 = that;
            }
            else {
                o1 = that;
                o2 = this;
            }
            synchronized (o1) {
                synchronized (o2) {
                    int cmp = AddressComparator.INSTANCE.compare(
                            o1.remoteServerSocketAddress,
                            o2.remoteServerSocketAddress);
                    if (cmp == 0) {
                        cmp = AddressComparator.INSTANCE.compare(
                                o1.clientSocketAddress, o2.clientSocketAddress);
                    }
                    return o1 == this
                            ? cmp
                            : -cmp;
                }
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#hashCode()
         */
        @Override
        @GuardedBy("this")
        public synchronized int hashCode() {
            return remoteServerSocketAddress.hashCode()
                    ^ clientSocketAddress.hashCode();
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#equals(java.lang.Object)
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
            final Stream that = (Stream) obj;
            return compareTo(that) == 0;
        }
    }

    /**
     * The logging service.
     */
    private static final Logger     logger             = Util.getLogger();
    /**
     * The various streams.
     */
    private static final int        NOTICE             = 0;
    private static final int        REQUEST            = 1;
    private static final int        DATA               = 2;
    static final int                STREAM_COUNT       = 3;
    /**
     * The list of {@link Stream}s that constitute a {@link Connection}.
     */
    @GuardedBy("this")
    private final List<Stream>      streams            = new ArrayList<Stream>(
                                                               STREAM_COUNT);
    /**
     * The address of the local server's socket.
     */
    private final InetSocketAddress localServerSocketAddress;
    /**
     * The socket timeout in milliseconds.
     */
    static final int                SO_TIMEOUT;
    /**
     * The name of the socket-timeout user-preference ({@value} ).
     */
    static final String             SO_TIMEOUT_KEY     = "socket timeout in milliseconds";
    /**
     * The default value for the socket-timeout user-preference ({@value} ).
     */
    static final int                SO_TIMEOUT_DEFAULT = 30000;

    static {
        final Preferences prefs = Preferences
                .userNodeForPackage(Connection.class);

        SO_TIMEOUT = prefs.getInt(SO_TIMEOUT_KEY, SO_TIMEOUT_DEFAULT);
        if (SO_TIMEOUT < 0) {
            throw new IllegalArgumentException("Invalid preference: \""
                    + SO_TIMEOUT_KEY + "\"=" + SO_TIMEOUT);
        }
    }

    /**
     * Constructs from the address of the local server's socket.
     * 
     * @param localServerSocketAddress
     *            The address of the local server's socket.
     * @throws NullPointerException
     *             if {@code localServerSocketAddress == null}.
     */
    protected Connection(final InetSocketAddress localServerSocketAddress) {
        if (localServerSocketAddress == null) {
            throw new NullPointerException();
        }
        this.localServerSocketAddress = localServerSocketAddress;
    }

    /**
     * Vets a {@link Connection#Stream}. Ensures that it connects to the same
     * remote site and that the client-side port number is distinct.
     * 
     * @param stream
     *            The {@link Connection#Stream} to be vetted.
     * @throws IllegalArgumentException
     *             if the remote IP address of the {@link Connection#Stream}
     *             doesn't equal that of the previously-added
     *             {@link Connection#Stream}.
     * @throws NullPointerException
     *             if {@code Stream == null}.
     */
    @GuardedBy("this")
    private synchronized void vetStream(final Stream stream) {
        if (streams.size() > 0) {
            final InetAddress actualInetAddress = stream.socket
                    .getInetAddress();
            final InetAddress expectedInetAddress = streams.get(0)
                    .getInetAddress();

            if (!actualInetAddress.equals(expectedInetAddress)) {
                throw new IllegalArgumentException(
                        "Different remote Internet addresses: "
                                + actualInetAddress + " != "
                                + expectedInetAddress);
            }

            final int actualClientPort = stream.getClientPort();
            for (final Stream extantStream : streams) {
                final int extantClientPort = extantStream.getClientPort();
                if (actualClientPort == extantClientPort) {
                    throw new IllegalArgumentException(
                            "Identical client port number: "
                                    + +extantClientPort);
                }
            }
        }
    }

    /**
     * Adds a {@link Connection#Stream} to the set of {@link Connection#Stream}.
     * 
     * @param stream
     *            The {@link Connection#Stream} to be added.
     * @throws IllegalArgumentException
     *             if the remote IP address of the {@link Connection#Stream}
     *             doesn't equal that of the previously-added sockets.
     * @throws IndexOutOfBoundsException
     *             if the set of {@link Connection#Stream}s is already complete.
     * @throws NullPointerException
     *             if {@code stream} is {@code null}.
     */
    @GuardedBy("this")
    protected synchronized void add(final Stream stream) {
        if (streams.size() >= STREAM_COUNT) {
            throw new IndexOutOfBoundsException();
        }

        vetStream(stream);
        streams.add(stream);

        if (streams.size() == STREAM_COUNT) {
            Collections.sort(streams);
        }
    }

    /**
     * Returns the current number of {@link Connection#Stream}s.
     * 
     * @return The current number of {@link Connection#Stream}.
     */
    @GuardedBy("this")
    protected synchronized int size() {
        return streams.size();
    }

    /**
     * Indicates if this instance is ready.
     * 
     * @return {@code true} if and only if this instance is ready.
     */
    @GuardedBy("this")
    protected synchronized boolean isReady() {
        return streams.size() == STREAM_COUNT;
    }

    /**
     * Returns the i-th stream.
     * 
     * @param index
     *            The index of the stream to be returned.
     * @return The i-th stream.
     */
    @GuardedBy("this")
    protected synchronized Connection.Stream getStream(final int index) {
        return streams.get(index);
    }

    /**
     * Returns the request-stream.
     * 
     * @return The request-stream.
     */
    protected Stream getRequestStream() {
        return getStream(REQUEST);
    }

    /**
     * Returns the notice-stream.
     * 
     * @return The notice-stream.
     */
    protected Stream getNoticeStream() {
        return getStream(NOTICE);
    }

    /**
     * Returns the data-stream.
     * 
     * @return The data-stream.
     */
    protected Stream getDataStream() {
        return getStream(DATA);
    }

    /**
     * Returns the i-th output stream.
     * 
     * @param i
     *            Index of the output stream to be returned.
     * @return The {@code i}-th output stream.
     */
    @GuardedBy("this")
    synchronized Stream.Output getOutputStream(final int i) {
        return streams.get(i).getOutput();
    }

    /**
     * Returns the i-th input stream.
     * 
     * @param i
     *            Index of the input stream to be returned.
     * @return The {@code i}-th input stream.
     */
    @GuardedBy("this")
    synchronized Stream.Input getInputStream(final int i) {
        return streams.get(i).getInput();
    }

    /**
     * Returns the i-th socket.
     * 
     * @return The i-th socket.
     * @throws IndexOutOfBoundsException
     *             if {@code i < 0 || i >= }{@link #getStreamCount}.
     */
    @GuardedBy("this")
    protected synchronized Socket getSocket(final int i) {
        return streams.get(i).getSocket();
    }

    /**
     * Returns the Internet address of the remote end of the connection.
     * 
     * @return the Internet address of the remote end of the connection.
     */
    protected InetAddress getRemoteInetAddress() {
        return streams.get(0).socket.getInetAddress();
    }

    /**
     * Returns the address of the remote server's socket.
     * 
     * @return the address of the remote server's socket.
     */
    protected InetSocketAddress getRemoteServerSocketAddress() {
        return streams.get(0).remoteServerSocketAddress;
    }

    /**
     * Closes all streams.
     */
    @GuardedBy("this")
    synchronized void close() {
        for (final Stream stream : streams) {
            stream.close();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @GuardedBy("this")
    @Override
    public final synchronized int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((streams == null)
                ? 0
                : getRemoteServerSocketAddress().hashCode());
        // streams.hashCode());
        return result;
    }

    @Override
    public final int compareTo(final Connection that) {
        if (this == that) {
            return 0;
        }
        int cmp;
        Connection o1, o2;
        if (System.identityHashCode(this) < System.identityHashCode(that)) {
            o1 = this;
            o2 = that;
        }
        else {
            o1 = that;
            o2 = this;
        }
        synchronized (o1) {
            synchronized (o2) {
                cmp = AddressComparator.INSTANCE.compare(
                        o1.localServerSocketAddress,
                        o2.localServerSocketAddress);
                if (cmp == 0) {
                    cmp = AddressComparator.INSTANCE.compare(
                            o1.getRemoteServerSocketAddress(),
                            o2.getRemoteServerSocketAddress());
                }
            }
        }
        return this == o1
                ? cmp
                : -cmp;
    }

    /**
     * Indicates if this instance equals an object. Two {@link Connection}s are
     * equal if they connect the same two nodes (i.e., if the socket addresses
     * of their local servers are equal and the socket addresses of their remote
     * servers are equal).
     * 
     * @param obj
     *            An object to test for equality.
     * @return {@code true} if and only if this instance is considered equal to
     *         the object.
     */
    @Override
    public final boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Connection that = (Connection) obj;
        return compareTo(that) == 0;
    }

    @Override
    public abstract String toString();
}
