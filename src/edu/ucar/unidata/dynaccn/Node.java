/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
     * The executor service.
     */
    private final ExecutorService                 executorService   = Executors
                                                                            .newCachedThreadPool();
    /**
     * The task completion service.
     */
    private final ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<Void>(
                                                                            executorService);
    /**
     * The clearing house for data.
     */
    private final ClearingHouse                   clearingHouse;
    /**
     * The server.
     */
    private final Server                          server;
    /**
     * The clients.
     */
    private final List<Client>                    clients           = new LinkedList<Client>();
    /**
     * Whether or not {@link #call} has been called}.
     */
    private boolean                               callCalled;

    /**
     * Constructs from the pathname of the file-tree and a specification of the
     * locally-desired data.
     * 
     * @param rootDir
     *            Pathname of the root of the file-tree.
     * @param predicate
     *            Specification of the locally-desired data.
     * @throws IOException
     *             if the server can't connect to the network.
     * @throws NullPointerException
     *             if {@code rootDir == null || predicate == null}.
     */
    Node(final File rootDir, final Predicate predicate) throws IOException {
        clearingHouse = new ClearingHouse(rootDir, predicate);
        server = new Server(clearingHouse);
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
     * Executes this instance. Returns normally if and only if the server and
     * all clients terminate normally.
     * 
     * @throws ExecutionException
     *             if the server or a client terminated due to an error.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws RejectedExecutionException
     *             if not enough threads are available.
     */
    @Override
    public Void call() throws InterruptedException, ExecutionException {
        callCalled = true;
        final Future<Void> serverFuture = completionService.submit(server);
        try {
            try {
                for (final Client client : clients) {
                    completionService.submit(client);
                }
                for (int i = 0; i < clients.size() + 1; i++) {
                    final Future<Void> future = completionService.take();
                    future.get();
                }
            }
            finally {
                executorService.shutdownNow();
            }
        }
        finally {
            serverFuture.cancel(true);
        }
        return null;
    }
}
