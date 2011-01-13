/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.net.SocketException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A localServer and no clients.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class SourceNode extends AbstractNode {
    /**
     * Causes the localServer to be notified of newly-created files in the file-tree.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private final class FileWatcher implements Callable<Void> {
        @Override
        public Void call() throws InterruptedException, IOException {
            archive.watchArchive(localServer);
            return null;
        }
    }

    private static Logger logger = LoggerFactory.getLogger(SourceNode.class);

    /**
     * Constructs from the data archive and a specification of the
     * locally-desired data.
     * 
     * @param archive
     *            The data archive.
     * @param predicate
     *            Specification of the locally-desired data.
     * @throws IOException
     *             if the localServer can't connect to the network.
     * @throws NullPointerException
     *             if {@code rootDir == null || predicate == null}.
     */
    SourceNode(final Archive archive, final Predicate predicate)
            throws IOException {
        this(archive, predicate, (int[]) null);
    }

    /**
     * Constructs from the data archive, a specification of the locally-desired
     * data, and the port numbers for the localServer.
     * 
     * @param archive
     *            The data archive.
     * @param predicate
     *            Specification of the locally-desired data.
     * @param serverPorts
     *            Port numbers for the localServer or {@code null}. A port number of
     *            zero will cause the operating-system to assign an ephemeral
     *            port. If {@code null} then all ports used by the localServer will
     *            be ephemeral.
     * @throws IOException
     *             if the localServer can't connect to the network.
     * @throws NullPointerException
     *             if {@code rootDir == null || predicate == null}.
     */
    SourceNode(final Archive archive, final Predicate predicate,
            final int[] serverPorts) throws IOException {
        super(archive, predicate, serverPorts);
    }

    /**
     * Constructs from the data archive, a specification of the locally-desired
     * data, and a range for the port numbers for the localServer.
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
     * @throws IOException
     *             if an unused port in the given range couldn't be found.
     * @throws NullPointerException
     *             if {@code rootDir == null || predicate == null || portSet ==
     *             null}.
     * @throws SocketException
     *             if a localServer-side socket couldn't be created.
     */
    SourceNode(final Archive archive, final Predicate predicate,
            final PortNumberSet portSet) throws IOException {
        super(archive, predicate, portSet);
    }

    /**
     * Executes this instance. Returns normally if all tasks complete normally
     * or if the current thread is interrupted.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     *             if a serious I/O error occurs.
     */
    @Override
    public Void call() throws InterruptedException, IOException {
        final String origThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(toString());
        /*
         * The {@link ExecutorService} for the localServer and file-watcher tasks.
         */
        final ExecutorService executorService = new ThreadPoolExecutor(2, 2, 0,
                TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
        final ExecutorCompletionService<Void> taskManager = new ExecutorCompletionService<Void>(
                executorService);
        try {
            taskManager.submit(localServer);
            taskManager.submit(new FileWatcher());
            final Future<Void> future = taskManager.take();
            try {
                future.get();
            }
            catch (final ExecutionException e) {
                final Throwable cause = e.getCause();
                if (cause instanceof InterruptedException) {
                    throw (InterruptedException) cause;
                }
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                }
                throw Util.launderThrowable(cause);
            }
            catch (final InterruptedException e) {
                logger.debug("Interrupted");
                throw e;
            }
        }
        finally {
            executorService.shutdownNow();
            Thread.currentThread().setName(origThreadName);
        }
        return null;
    }

    /**
     * Returns a byte channel to a file that will be published.
     * 
     * @param path
     *            Pathname of the file relative to the root of the file-tree.
     * @return A byte channel to the file.
     * @throws IOException
     *             if an I/O error occurs.
     */
    SeekableByteChannel newBytechannel(Path path) throws IOException {
        path = archive.getHiddenForm(path);
        Files.createDirectories(path.getParent());
        final SeekableByteChannel channel = path.newByteChannel(
                StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        return channel;
    }

    /**
     * Returns a handle on a new, unpublished file that's ready for content.
     * 
     * @param path
     *            The pathname of the file relative to the root of the
     *            file-tree.
     * @return A handle on the new file.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws IllegalArgumentException
     *             if {@code path.isAbsolute()}.
     */
    PubFile newPubFile(final Path path) throws IOException {
        return new PubFile(archive, path);
    }

    /**
     * Returns the number of clients that this instance is serving.
     * 
     * @return The number of clients that this instance is serving.
     */
    int getClientCount() {
        return clearingHouse.getPeerCount();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "SourceNode [archive=" + archive + "]";
    }
}
