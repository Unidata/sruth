/**
 * Copyright 2011 University Corporation for Atmospheric Research.  All rights reserved.
 * See file LICENSE.txt in the top-level directory for licensing information.
 */
package edu.ucar.unidata.sruth;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicReference;

import net.jcip.annotations.GuardedBy;

import org.slf4j.Logger;

import edu.ucar.unidata.sruth.Connection.Stream;

/**
 * Connects to a remote server and exchanges data.
 * 
 * @author Steven R. Emmerson
 */
final class Client extends UninterruptibleTask<Boolean> {
    /**
     * The logging service.
     */
    private static final Logger                       logger        = Util.getLogger();
    /**
     * The data clearing-house.
     */
    private final ClearingHouse                       clearingHouse;
    /**
     * Address of the remote server.
     */
    private final InetSocketAddress                   remoteServer;
    /**
     * The data-selection filter.
     */
    private final Filter                              filter;
    /**
     * Factory for creating {@link Connection}s.
     */
    private final ConnectionFactory                   connectionFactory;
    /**
     * The corresponding peer.
     */
    @GuardedBy("this")
    private Peer                                      peer;
    /**
     * The underlying connection to the server.
     */
    private final AtomicReference<ConnectionToServer> connectionRef = new AtomicReference<ConnectionToServer>();

    /**
     * Constructs from the address of the local server, the address of a remote
     * server, the data-selection filter, and the data-exchange clearing house
     * to be used.
     * 
     * @param localServer
     *            Address of the local server.
     * @param remoteServer
     *            Address of a remote server.
     * @param filter
     *            The data-selection filter.
     * @param clearingHouse
     *            The clearing-house to use.
     * @throws NullPointerException
     *             if {@code localServer == null}.
     * @throws NullPointerException
     *             if {@code remoteServer == null}.
     * @throws NullPointerException
     *             if {@code filter == null}.
     * @throws NullPointerException
     *             if {@code clearingHouse == null}.
     */
    Client(final InetSocketAddress localServer,
            final InetSocketAddress remoteServer, final Filter filter,
            final ClearingHouse clearingHouse) {
        if (null == localServer || null == clearingHouse || null == filter
                || null == remoteServer) {
            throw new NullPointerException();
        }

        this.remoteServer = remoteServer;
        this.filter = filter;
        this.clearingHouse = clearingHouse;
        connectionFactory = new ConnectionFactory(localServer);
    }

    /**
     * Returns the address of the remote server to which this instance will
     * connect or has connected.
     * 
     * @return the address of the remote server.
     */
    InetSocketAddress getServerAddress() {
        return remoteServer;
    }

    /**
     * Executes this instance. Returns if this instance would duplicate an
     * existing connection, when all data that can be received has been
     * received, or if an exception is thrown.
     * <p>
     * This is a potentially uninterruptible and indefinite operation.
     * 
     * @returns {@code false} if this instance would duplicate a previously
     *          existing connection; otherwise {@code true}.
     * @throws ConnectException
     *             if the connection to the remote server can't be made.
     * @throws EOFException
     *             if the remote server closes the connection.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if a serious I/O error occurs.
     * @throws SocketException
     *             if the remote peer couldn't be accessed.
     */
    @Override
    public Boolean call() throws ConnectException, EOFException, IOException,
            SocketException, InterruptedException {
        logger.debug("Starting up: {}", this);
        final String origName = Thread.currentThread().getName();
        final Thread currentThread = Thread.currentThread();
        currentThread.setName(toString());

        try {
            final ConnectionToServer connection;
            connection = connectionFactory.getInstance(remoteServer);
            connectionRef.set(connection);
            connection.open();

            try {
                final Stream requestStream = connection.getRequestStream();

                try {
                    // NB: Client sends filter first, then server.
                    requestStream.send(filter);
                }
                catch (final IOException e) {
                    throw (ConnectException) new ConnectException(
                            "Couldn't send filter on " + connection)
                            .initCause(e);
                }

                final Filter serverFilter;
                try {
                    serverFilter = (Filter) requestStream
                            .receiveObject(Connection.SO_TIMEOUT);
                }
                catch (final IOException e) {
                    throw (ConnectException) new ConnectException(
                            "Couldn't receive filter on " + connection)
                            .initCause(e);
                }
                catch (final ClassNotFoundException e) {
                    throw (IOException) new IOException(
                            "Invalid server response on " + connection)
                            .initCause(e);
                }
                catch (final ClassCastException e) {
                    throw (IOException) new IOException(
                            "Invalid server response on " + connection)
                            .initCause(e);
                }

                synchronized (this) {
                    peer = new Peer(clearingHouse, connection, filter,
                            serverFilter);
                }
                final Boolean isValid = peer.call();
                if (!isValid) {
                    logger.debug("Invalid server: {}", this);
                }
                return isValid;
            }
            finally {
                connection.close();
            }
        }
        finally {
            Thread.currentThread().setName(origName);
            logger.trace("Done: {}", this);
        }
    }

    @Override
    protected void stop() {
        final ConnectionToServer connection = connectionRef.get();
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * Returns the number of bytes downloaded since the later of the previous
     * call or the start of this client.
     * 
     * @return The number of downloaded bytes since last time or the start.
     */
    synchronized long getDownloadAmount() {
        // TODO
        return 0;
    }

    /**
     * Returns the number of octets downloaded from the server since the most
     * recent call to {@link #call()} or {@link #restartCounter()}.
     * 
     * @return The number of downloaded octets.
     */
    long getCounter() {
        return peer.getCounter();
    }

    /**
     * Resets and restarts the counter.
     */
    void restartCounter() {
        peer.restartCounter();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Client [remoteServer=" + remoteServer + ", localPredicate="
                + clearingHouse.getPredicate() + "]";
    }
}
