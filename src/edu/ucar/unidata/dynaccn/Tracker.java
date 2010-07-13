/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.NotThreadSafe;
import net.jcip.annotations.ThreadSafe;

/**
 * Keeps track of subscriptions by sink-nodes and gives sink-nodes contact
 * information for the source-node and/or other sink-nodes.
 * 
 * Instances are thread-compatible but not thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@NotThreadSafe
final class Tracker extends BlockingTask<Void> {
    /**
     * A map whose keys are file-selection filters and whose values are the
     * servers that receive the files specified by the keys.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private static final class FilterServersMap {
        /**
         * The actual map.
         */
        @GuardedBy("this")
        private final Map<Filter, Set<ServerInfo>> map = new HashMap<Filter, Set<ServerInfo>>();

        /**
         * Returns the set of servers that can satisfy a given file-selection
         * filter or {@code null}. The set is backed by this instance, so
         * modifying it will modify this instance.
         * 
         * @param filter
         *            The filter to satisfy.
         * @return The set of satisfying servers or {@code null} if no such set
         *         exists.
         */
        synchronized Set<ServerInfo> get(final Filter filter) {
            // TODO: support subsets and supersets; not just equality
            return map.get(filter);
        }

        /**
         * Adds to a mapping from a filter to a server. Creates the entry if it
         * doesn't already exist.
         * 
         * @param filter
         *            The file-selection filter.
         * @param serverInfo
         *            Information on the server.
         */
        synchronized void add(final Filter filter, final ServerInfo serverInfo) {
            Set<ServerInfo> serverInfos = map.get(filter);
            if (null == serverInfos) {
                serverInfos = new TreeSet<ServerInfo>();
                map.put(filter, serverInfos);
            }
            serverInfos.add(serverInfo);
        }
    }

    /**
     * Handles one and only one sink-node.
     * 
     * Instances are thread-compatible but not thread-safe.
     */
    @NotThreadSafe
    class Trackerlet extends BlockingTask<Void> implements Callable<Void> {
        /**
         * The socket.
         */
        private final Socket socket;

        /**
         * Constructs from a socket.
         * 
         * @param socket
         *            The socket.
         * @throws NullPointerException
         *             if {@code socket == null}.
         */
        Trackerlet(final Socket socket) {
            if (null == socket) {
                throw new NullPointerException();
            }
            this.socket = socket;
        }

        /**
         * @throws ClassNotFoundException
         *             if something unknown is read from the socket.
         * @throws IOException
         *             if an I/O error occurs.
         */
        @Override
        public Void call() throws IOException, ClassNotFoundException {
            // TODO: set time-limit interruption
            final InputStream inputStream = socket.getInputStream();
            final ObjectInputStream ois = new ObjectInputStream(inputStream);
            final Inquisitor inquisitor = (Inquisitor) ois.readObject();
            final OutputStream outputStream = socket.getOutputStream();
            final ObjectOutputStream oos = new ObjectOutputStream(outputStream);
            inquisitor.process(Tracker.this, oos);
            oos.close();
            ois.close();
            socket.close();
            return null;
        }

        @Override
        protected void stop() {
            try {
                socket.close();
            }
            catch (final IOException ignored) {
            }
        }
    }

    /**
     * Executes each task for a single sink-node.
     */
    private final ExecutorService  executorService  = new CancellingExecutor(
                                                            0,
                                                            25,
                                                            60,
                                                            TimeUnit.SECONDS,
                                                            new SynchronousQueue<Runnable>());
    /**
     * The server socket on which this instance listens.
     */
    private final ServerSocket     serverSocket;
    /**
     * The filter-to-servers map.
     */
    private final FilterServersMap filterServersMap = new FilterServersMap();
    /**
     * Information on the source-server.
     */
    private final ServerInfo       sourceServer;

    /**
     * Constructs from information on the source-server.
     * 
     * @param sourceServer
     *            Information on the source-server.
     * @throws IOException
     *             if a server serverSocket can't be created.
     * @throws NullPointerException
     *             if {@code sourceServer == null}.
     */
    Tracker(final ServerInfo sourceServer) throws IOException {
        if (null == sourceServer) {
            throw new NullPointerException();
        }
        serverSocket = new ServerSocket();
        try {
            serverSocket.bind(new InetSocketAddress(0));
        }
        catch (final IOException e) {
            try {
                serverSocket.close();
            }
            catch (final Exception ignored) {
            }
            throw e;
        }
        this.sourceServer = sourceServer;
    }

    /**
     * Returns the local address of the server socket.
     * 
     * @return The local address of the server socket.
     */
    InetSocketAddress getAddress() {
        return (InetSocketAddress) serverSocket.getLocalSocketAddress();
    }

    /**
     * Executes this instance. Completes normally if and only if the current
     * thread is interrupted. Closes the server socket.
     * 
     * @throws IOException
     *             if an I/O error occurs.
     */
    public Void call() throws IOException {
        final String origThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(toString());
        try {
            for (;;) {
                try {
                    final Socket socket = serverSocket.accept();
                    final Trackerlet trackerlet = new Trackerlet(socket);
                    executorService.submit(trackerlet);
                }
                catch (final IOException e) {
                    if (!Thread.currentThread().isInterrupted()) {
                        throw e;
                    }
                    // Implements thread interruption policy
                    return null;
                }
            }
        }
        finally {
            executorService.shutdownNow();
            try {
                serverSocket.close();
            }
            catch (final IOException ignored) {
            }
            Thread.currentThread().setName(origThreadName);
        }
    }

    @Override
    protected void stop() {
        executorService.shutdownNow();
        try {
            serverSocket.close();
        }
        catch (final IOException ignored) {
        }
    }

    /**
     * Returns an object that will connect a sink-node to the servers that will
     * satisfy its data-selection predicate.
     * 
     * @param predicate
     *            The file-selection predicate to satisfy.
     * @return A object that will appropriately connect a sink-node.
     */
    Plumber getPlumber(final Predicate predicate) {
        final Plumber plumber = new Plumber();
        for (final Filter filter : predicate) {
            final Set<ServerInfo> servers = filterServersMap.get(filter);
            for (final ServerInfo serverInfo : servers) {
                plumber.add(serverInfo, filter);
            }
        }
        // TODO: don't always add the source-server
        plumber.put(sourceServer, predicate);
        return plumber;
    }

    /**
     * Registers a server capable of satisfying a given file-selection
     * predicate.
     * 
     * @param serverInfo
     *            Information on the server.
     * @param predicate
     *            The file-selection predicate.
     * @throws NullPointerException
     *             if {@code serverInfo == null || predicate == null}.
     */
    void register(final ServerInfo serverInfo, final Predicate predicate) {
        for (final Filter filter : predicate) {
            filterServersMap.add(filter, serverInfo);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Tracker [serverSocket=" + serverSocket + ", sourceServer="
                + sourceServer + "]";
    }
}
