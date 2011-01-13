/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.prefs.Preferences;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A local server and one or more clients.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class SinkNode extends AbstractNode {
    /**
     * The task execution service.
     */
    private final ExecutorService                 executor                        = Executors
                                                                                          .newCachedThreadPool();
    /**
     * The task completion service.
     */
    private final ExecutorCompletionService<Void> completionService               = new ExecutorCompletionService<Void>(
                                                                                          executor);
    /**
     * The plumber for obtaining servers.
     */
    private final AbstractPlumber                 plumber;
    /**
     * The set of inactive servers.
     */
    @GuardedBy("this")
    private final Set<ServerInfo>                 inactiveServers                 = new TreeSet<ServerInfo>();
    /**
     * The set of active servers.
     */
    @GuardedBy("this")
    private final Set<ServerInfo>                 activeServers                   = new TreeSet<ServerInfo>();
    /**
     * The set of offline servers.
     */
    @GuardedBy("this")
    private final Set<ServerInfo>                 offlineServers                  = new TreeSet<ServerInfo>();
    /**
     * The logging service.
     */
    private static final Logger                   logger                          = LoggerFactory
                                                                                          .getLogger(Client.class);
    /**
     * The preferences key for the nominal number of remote servers.
     */
    private static final String                   NOMINAL_REMOTE_SERVER_COUNT_KEY = "nominalRemoteServerCount";
    /**
     * The nominal number of remote servers.
     */
    private static final int                      NOMINAL_REMOTE_SERVER_COUNT;

    static {
        final Preferences prefs = Preferences
                .userNodeForPackage(SinkNode.class);
        NOMINAL_REMOTE_SERVER_COUNT = prefs.getInt(
                NOMINAL_REMOTE_SERVER_COUNT_KEY, 8);
        if (NOMINAL_REMOTE_SERVER_COUNT <= 0) {
            throw new IllegalArgumentException("Invalid \""
                    + NOMINAL_REMOTE_SERVER_COUNT_KEY + "\" preference: "
                    + NOMINAL_REMOTE_SERVER_COUNT);
        }
    }

    /**
     * Constructs from the data archive, a specification of the locally-desired
     * data, and the factory for creating sets of informations on remote
     * servers.
     * 
     * @param archive
     *            The data archive.
     * @param predicate
     *            Specification of the locally-desired data.
     * @param plumber
     *            The object that tells this instance about remote servers that
     *            can satisfy this instance's data-selection predicate.
     * @throws IOException
     *             if the local server can't connect to the network.
     * @throws NullPointerException
     *             if {@code rootDir == null || predicate == null}.
     */
    SinkNode(final Archive archive, final Predicate predicate,
            final AbstractPlumber plumber) throws IOException {
        this(archive, predicate, PortNumberSet.ZERO, plumber);
    }

    /**
     * Constructs from the data archive, a specification of the locally-desired
     * data, and a range of port numbers for the local server.
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
     * @param plumber
     *            The object that tells this instance about remote servers that
     *            can satisfy this instance's data-selection predicate.
     * @throws IllegalArgumentException
     *             if {@code minPort > maxPort}.
     * @throws IOException
     *             if an unused port in the given range couldn't be found.
     * @throws NullPointerException
     *             if {@code rootDir == null || predicate == null || portSet ==
     *             null || plumber == null}.
     * @throws SocketException
     *             if a server socket couldn't be created.
     */
    SinkNode(final Archive archive, final Predicate predicate,
            final PortNumberSet portSet, final AbstractPlumber plumber)
            throws IOException {
        super(archive, predicate, portSet);
        if (plumber == null) {
            throw new NullPointerException();
        }
        this.plumber = plumber;
    }

    /**
     * Adds a server from which to obtain data. Doesn't add the server if it has
     * already been added.
     * 
     * @param serverInfo
     *            Information on the server to add.
     * @return {@code true} if and only if the server was added.
     */
    synchronized boolean add(final ServerInfo serverInfo) {
        if (inactiveServers.contains(serverInfo)
                || activeServers.contains(serverInfo)
                || offlineServers.contains(serverInfo)) {
            return false;
        }
        inactiveServers.add(serverInfo);
        return true;
    }

    /**
     * Moves information on a remote server from one set to another.
     * 
     * @param sourceSet
     *            The set that contains the information to be moved.
     * @param serverInfo
     *            The information to be moved.
     * @param destSet
     *            The set to which the information will be moved.
     */
    private synchronized void move(final Set<ServerInfo> sourceSet,
            final ServerInfo serverInfo, final Set<ServerInfo> destSet) {
        sourceSet.remove(serverInfo);
        destSet.add(serverInfo);
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
            final Client client = new Client(serverInfo,
                    localServer.getPorts(), clearingHouse);
            completionService.submit(new Callable<Void>() {
                public Void call() throws ConnectException,
                        InterruptedException, IOException {
                    try {
                        logger.info("Starting up: {}", client);
                        client.call();
                        logger.info("Terminated: {}", client);
                        move(activeServers, serverInfo, inactiveServers);
                    }
                    catch (final ConnectException e) {
                        logger.info("Connection lost: {}", client);
                        move(activeServers, serverInfo, offlineServers);
                        throw e;
                    }
                    catch (final IOException e) {
                        logger.error("Failure: " + client, e);
                        move(activeServers, serverInfo, inactiveServers);
                        throw e;
                    }
                    catch (final InterruptedException e) {
                        logger.info("Interrupted: {}", client);
                        move(activeServers, serverInfo, inactiveServers);
                        throw e;
                    }
                    return null;
                }
            });
        }
    }

    /**
     * Submits clients for execution. Checks the number of active and inactive
     * servers and contacts the tracker to get more (inactive) servers if
     * necessary. Takes servers from the inactive pool and creates clients for
     * them until the inactive pool is empty or the nominal number of clients is
     * reached (whichever comes first).
     * 
     * @throws ClassNotFoundException
     *             if the reply from the tracker is not understood.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private synchronized void startClients() throws ClassNotFoundException,
            IOException {
        if (activeServers.size() < NOMINAL_REMOTE_SERVER_COUNT) {
            plumber.connect(this);
        }

        for (final Iterator<ServerInfo> iter = inactiveServers.iterator(); iter
                .hasNext()
                && activeServers.size() < NOMINAL_REMOTE_SERVER_COUNT;) {
            final ServerInfo serverInfo = iter.next();
            submitClient(serverInfo);
            activeServers.add(serverInfo);
            iter.remove();
        }
    }

    /**
     * Executes this instance. Returns normally if and only if all data was
     * received. Replaces clients when appropriate.
     * 
     * @throws ClassNotFoundException
     *             if a problem occurs getting a result from the Tracker.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if an I/O error occurs.
     */
    public Void call() throws ClassNotFoundException, IOException,
            InterruptedException {
        try {
            completionService.submit(localServer);
            startClients();
            for (;;) {
                final Future<Void> future = completionService.take();
                try {
                    future.get();
                    /*
                     * The local server won't return; therefore, a client must
                     * have determined that all data has been received.
                     */
                    break;
                }
                catch (final ExecutionException e) {
                    final Throwable cause = e.getCause();
                    if (cause instanceof ConnectException) {
                        startClients();
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
        return "SinkNode [activeServers=(" + activeServers.size() + "),"
                + "inactiveServers=(" + inactiveServers.size() + "),"
                + "offlineServers=(" + offlineServers.size() + ")]";
    }
}
