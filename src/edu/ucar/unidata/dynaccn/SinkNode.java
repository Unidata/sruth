/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A server and one or more clients.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class SinkNode extends AbstractNode {
    /**
     * A task for executing a client.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private final class ClientTask extends FutureTask<Void> implements
            Callable<Void> {
        /**
         * The associated client.
         */
        private final Client client;

        /**
         * Constructs from a client.
         * 
         * @param client
         *            The client.
         * @throws NullPointerException
         *             if {@client == null}.
         */
        ClientTask(final Client client) {
            super(client);
            this.client = client;
        }

        /**
         * Submits an identical client for execution if appropriate.
         */
        @Override
        protected void done() {
            if (!isCancelled()) {
                try {
                    get();
                }
                catch (final InterruptedException e) {
                    // done() implies get() must immediately return
                    throw new AssertionError(e);
                }
                catch (final ExecutionException e) {
                    final Throwable cause = e.getCause();
                    logger.error("Client " + client + " died", cause);
                    if (!(cause instanceof Error || cause instanceof RuntimeException)) {
                        /*
                         * A checked-exception means that it might work next
                         * time.
                         */
                        final ServerInfo serverInfo = client.getServerInfo();
                        try {
                            Thread.sleep(30000);
                            createClient(serverInfo);
                        }
                        catch (final InterruptedException ignored) {
                        }
                    }
                }
            }
        }

        @Override
        public Void call() {
            run();
            return null;
        }
    }

    /**
     * The logging service.
     */
    private static final Logger                   logger            = LoggerFactory
                                                                            .getLogger(Client.class);
    /**
     * Information on the servers to which to connect.
     */
    @GuardedBy("this")
    private final Set<ServerInfo>                 serverInfos       = new TreeSet<ServerInfo>();
    /**
     * The executor service for clients.
     */
    private final ExecutorService                 executor          = Executors
                                                                            .newCachedThreadPool();
    /**
     * The task completion service for clients.
     */
    private final ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<Void>(
                                                                            executor);

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
    SinkNode(final Archive archive, final Predicate predicate)
            throws IOException {
        super(archive, predicate);
    }

    /**
     * Adds a server from which to obtain data.
     * 
     * @param serverInfo
     *            Information on the server to add.
     */
    synchronized void add(final ServerInfo serverInfo) {
        serverInfos.add(serverInfo);
    }

    /**
     * Creates a client. Does all the necessary bookkeeping.
     * 
     * @param serverInfo
     *            Information on the server to which to connect.
     */
    private synchronized void createClient(final ServerInfo serverInfo) {
        final Client client = new Client(serverInfo, clearingHouse);
        final ClientTask clientTask = new ClientTask(client);
        completionService.submit(clientTask);
    }

    /**
     * Executes this instance. Returns normally if and only if all data was
     * received. Replaces clients when appropriate.
     * 
     * @throws ExecutionException
     *             if an insurmountable problem occurs during execution.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws RejectedExecutionException
     *             if not enough threads are available.
     */
    @Override
    public synchronized Void call() throws ExecutionException,
            InterruptedException {
        try {
            final Future<Void> serverFuture = completionService.submit(server);
            for (final ServerInfo serverInfo : serverInfos) {
                createClient(serverInfo);
            }
            for (;;) {
                final Future<Void> future = completionService.take();
                if (future.equals(serverFuture)) {
                    future.get(); // will throw an ExecutionException
                    throw new AssertionError();
                }
                try {
                    future.get();
                    // A client has determined that all data has been received
                    break;
                }
                catch (final ExecutionException e) {
                    final Throwable cause = e.getCause();
                    if (cause instanceof Error
                            || cause instanceof RuntimeException) {
                        /*
                         * A checked exception caused creation of an identical
                         * client; an unchecked exception means the client died
                         * a horrible death due to an insurmountable problem
                         * (e.g., NullPointerException).
                         */
                        throw e;
                    }
                }
            }
        }
        finally {
            executor.shutdownNow();
        }
        return null;
    }

    /**
     * Returns the number of received files since {@link #call()} was called.
     * 
     * @return The number of received files since {@link #call()} was called.
     */
    public long getReceivedFileCount() {
        return clearingHouse.getReceivedFileCount();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "SinkNode [serverInfos=(" + serverInfos.size() + ")]";
    }
}
