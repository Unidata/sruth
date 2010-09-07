/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

import net.jcip.annotations.ThreadSafe;

/**
 * A server and no clients.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class SourceNode extends AbstractNode {
    /**
     * The executor service used by all instances.
     */
    protected static final ExecutorService executorService = Executors
                                                                   .newCachedThreadPool();

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
    SourceNode(final Archive archive, final Predicate predicate)
            throws IOException {
        super(archive, predicate);
    }

    /**
     * Executes this instance. Returns normally if all tasks complete normally
     * or if the current thread is interrupted.
     * 
     * @throws ExecutionException
     *             if a task threw an exception.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws RejectedExecutionException
     *             if not enough threads are available.
     */
    @Override
    public Void call() throws ExecutionException, InterruptedException {
        final String origThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(toString());
        final TaskManager<Void> taskManager = new TaskManager<Void>(
                executorService);
        try {
            taskManager.submit(server);
            taskManager.submit(new FileWatcher());
            taskManager.waitUpon();
        }
        finally {
            taskManager.cancel();
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
        path = archive.hide(path);
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
