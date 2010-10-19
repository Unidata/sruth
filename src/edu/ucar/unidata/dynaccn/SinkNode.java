/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
     * The number of clients.
     */
    @GuardedBy("this")
    private int                                   clientCount;
    /**
     * The target number of clients.
     */
    @GuardedBy("this")
    private int                                   targetClientCount;
    /**
     * The sleep interval in seconds.
     */
    private final long                            sleepInterval     = 60;
    /**
     * The task execution service.
     */
    private final ExecutorService                 executor          = Executors
                                                                            .newCachedThreadPool();
    /**
     * The task completion service.
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
        this(archive, predicate, PortNumberSet.ZERO);
    }

    /**
     * Constructs from the data archive, a specification of the locally-desired
     * data, and a range of port numbers for the server.
     * 
     * If {@code minPort == 0 && maxPort == 0} then the operating-system will
     * assign ephemeral ports.
     * 
     * @param archive
     *            The data archive.
     * @param predicate
     *            Specification of the locally-desired data.
     * @param portSet
     *            The set of candidate port numbers.
     * @throws IllegalArgumentException
     *             if {@code minPort > maxPort}.
     * @throws IOException
     *             if an unused port in the given range couldn't be found.
     * @throws NullPointerException
     *             if {@code rootDir == null || predicate == null || portSet ==
     *             null}.
     * @throws SocketException
     *             if a server-side socket couldn't be created.
     */
    SinkNode(final Archive archive, final Predicate predicate,
            final PortNumberSet portSet) throws IOException {
        super(archive, predicate, portSet);
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
     * Submits a client task for execution. Does nothing if the task execution
     * service is shut down.
     * 
     * @param serverInfo
     *            Information on the server to which the client shall connect.
     */
    private synchronized void submitClient(final ServerInfo serverInfo) {
        if (!executor.isShutdown()) {
            final Client client = new Client(serverInfo, clearingHouse);
            completionService.submit(new Callable<Void>() {
                public Void call() throws ConnectException,
                        InterruptedException, IOException {
                    try {
                        client.call();
                        logger.debug("Client completed: {}", client);
                    }
                    catch (final ConnectException e) {
                        logger.info(e.toString());
                        Thread.sleep(30000);
                        submitClient(client.getServerInfo());
                        throw e;
                    }
                    catch (final IOException e) {
                        logger.error("Client failure: " + client, e);
                        throw e;
                    }
                    return null;
                }
            });
        }
    }

    /**
     * Executes this instance. Returns normally if and only if all data was
     * received. Replaces clients when appropriate.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if an I/O error occurs.
     */
    public Void call() throws IOException, InterruptedException {
        try {
            completionService.submit(server);
            synchronized (this) {
                for (final ServerInfo serverInfo : serverInfos) {
                    submitClient(serverInfo);
                }
            }
            for (;;) {
                final Future<Void> future = completionService.take();
                try {
                    future.get();
                    /*
                     * The server won't return; therefore, a client must have
                     * determined that all data has been received.
                     */
                    break;
                }
                catch (final ExecutionException e) {
                    final Throwable cause = e.getCause();
                    if (cause instanceof ConnectException) {
                        // task was resubmitted
                    }
                    else if (cause instanceof IOException) {
                        throw (IOException) cause;
                    }
                    else if (cause instanceof InterruptedException) {
                        break;
                    }
                    else {
                        throw Util.launderThrowable(cause);
                    }
                }
                catch (final InterruptedException e) {
                    logger.debug("Interrupted");
                    throw e;
                }
            }
        }
        finally {
            synchronized (this) {
                executor.shutdownNow();
            }
        }
        return null;
    }

    public Void call2() throws InterruptedException, IOException {
        final ExecutorService executor = Executors.newCachedThreadPool();
        final ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<Void>(
                executor);
        try {
            completionService.submit(server);
            for (;;) {
                synchronized (this) {
                    // TODO: TOO SIMPLISTIC: MUST ACCOMMODATE CLIENT TERMINATION
                    if (clientCount >= targetClientCount) {
                        cancelWorstClient();
                    }
                    if (clientCount < targetClientCount) {
                        startNewClients(completionService);
                        startDownloadCounters();
                    }
                }
                final Future<Void> future = completionService.poll(
                        sleepInterval, TimeUnit.SECONDS);
                if (future != null && !future.isCancelled()) {
                    try {
                        future.get();
                        /*
                         * The server won't return; therefore, a client must
                         * have determined that all data has been received.
                         */
                        break;
                    }
                    catch (final ExecutionException e) {
                        final Throwable cause = e.getCause();
                        if (cause instanceof ConnectException) {
                            // task was resubmitted
                        }
                        else if (cause instanceof IOException) {
                            throw (IOException) cause;
                        }
                        else if (cause instanceof InterruptedException) {
                            break;
                        }
                        else {
                            throw Util.launderThrowable(cause);
                        }
                    }
                    catch (final InterruptedException e) {
                        logger.debug("Interrupted");
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

    private void startDownloadCounters() {
        // TODO Auto-generated method stub
    }

    private void cancelWorstClient() {
        // TODO Auto-generated method stub
    }

    private synchronized void startNewClients(
            final ExecutorCompletionService<Void> completionService) {
        // TODO Auto-generated method stub
    }

    /**
     * Returns the number of received files since {@link #call()} was called.
     * 
     * @return The number of received files since {@link #call()} was called.
     */
    long getReceivedFileCount() {
        return clearingHouse.getReceivedFileCount();
    }

    /**
     * Returns the current number of peers to which this instance is connected.
     * 
     * @return The current number of peers to which this instance is connected.
     */
    int getPeerCount() {
        return clearingHouse.getPeerCount();
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
