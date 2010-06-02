/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

/**
 * A server and zero or more clients.
 * 
 * Instances are thread-compatible but not thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class Node implements Callable<Void> {
    /**
     * Causes the server to be notified of newly-created files in the file-tree.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private final class FileWatcher implements Callable<Void> {
        @Override
        public Void call() throws IOException {
            try {
                archive.watchArchive(server);
            }
            catch (final InterruptedException ignored) {
                // Implements thread interruption policy
            }
            return null;
        }
    }

    /**
     * The executor service used by all instances.
     */
    private static final ExecutorService executorService = Executors
                                                                 .newCachedThreadPool();
    /**
     * The data archive.
     */
    private final Archive                archive;
    /**
     * The clearing house for data.
     */
    private final ClearingHouse          clearingHouse;
    /**
     * The server.
     */
    private final Server                 server;
    /**
     * The clients.
     */
    private final List<Client>           clients         = new LinkedList<Client>();
    /**
     * Whether or not {@link #call} has been called}.
     */
    private volatile boolean             callCalled;

    /**
     * Constructs from the data archive and a specification of the
     * locally-desired data.
     * 
     * @param archive
     *            The data archive.
     * @param predicate
     *            Specification of the locally-desired data.
     * @throws IOException
     *             if the server can't connect to the network.
     * @throws NullPointerException
     *             if {@code rootDir == null || predicate == null}.
     */
    Node(final Archive archive, final Predicate predicate) throws IOException {
        clearingHouse = new ClearingHouse(archive, predicate);
        server = new Server(clearingHouse);
        this.archive = archive;
    }

    /**
     * Returns information on the server.
     * 
     * @return Information on the server.
     * @throws UnknownHostException
     *             if the IP address of the local host can't be obtained.
     */
    ServerInfo getServerInfo() throws UnknownHostException {
        return server.getServerInfo();
    }

    /**
     * Adds a server from which to obtain data.
     * 
     * @param serverInfo
     *            Information on the server to add.
     * @throws IllegalStateException
     *             if {@link #call} has been called.
     */
    void add(final ServerInfo serverInfo) {
        if (callCalled) {
            throw new IllegalStateException();
        }
        final Client client = new Client(serverInfo, clearingHouse);
        clients.add(client);
    }

    /**
     * Executes this instance. Returns normally if all tasks complete normally
     * or if the current thread is interrupted.
     * 
     * @throws ExecutionException
     *             if a task threw an exception.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws RejectedExecutionException
     *             if not enough threads are available.
     */
    @Override
    public Void call() throws ExecutionException, IOException {
        callCalled = true;
        final TaskManager<Void> taskManager = new TaskManager<Void>(
                executorService);
        try {
            taskManager.submit(server);
            if (Predicate.NOTHING.equals(clearingHouse.getPredicate())) {
                // This node wants nothing => it's a source-node
                taskManager.submit(new FileWatcher());
            }
            for (final Client client : clients) {
                taskManager.submit(client);
            }
            taskManager.waitUpon();
        }
        catch (final InterruptedException ignored) {
            // Implements thread interruption policy
        }
        finally {
            taskManager.cancel();
        }
        return null;
    }
}
