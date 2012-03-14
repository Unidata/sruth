/**
 * Copyright 2011 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.net.SocketException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import net.jcip.annotations.ThreadSafe;

/**
 * A top-level node of a distribution graph. A source-node has a {@link Server}
 * and a {@link FileWatcher} but no {@link ClientManager}-s.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class SourceNode extends AbstractNode {
    /**
     * Causes the local server to be notified of newly-created files in the
     * file-tree.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    private final class FileWatcher implements Callable<Void> {
        private final CountDownLatch isRunningLatch = new CountDownLatch(1);

        @Override
        public Void call() throws InterruptedException, IOException {
            isRunningLatch.countDown();
            getArchive().watchArchive(localServer);
            return null;
        }

        /**
         * Waits until this instance is running.
         * 
         * @throws InterruptedException
         *             if the current thread is interrupted.
         */
        void waitUntilRunning() throws InterruptedException {
            isRunningLatch.await();
        }
    }

    /**
     * The {@link ExecutorService} for the localServer and file-watcher tasks.
     */
    private final CancellingExecutor              executorService = new CancellingExecutor(
                                                                          2,
                                                                          2,
                                                                          0,
                                                                          TimeUnit.SECONDS,
                                                                          new SynchronousQueue<Runnable>());
    /**
     * The task completion service.
     */
    private final ExecutorCompletionService<Void> taskManager     = new ExecutorCompletionService<Void>(
                                                                          executorService);
    /**
     * The watcher for new files.
     */
    private final FileWatcher                     fileWatcher     = new FileWatcher();

    /**
     * Constructs from the data archive and a specification of the
     * locally-desired data. This constructor is equivalent to the constructor
     * {@link #SourceNode(Archive, InetSocketAddressSet) SourceNode(archive, new
     * InetSocketAddressSet())}.
     * 
     * @param archive
     *            The data archive.
     * @throws IOException
     *             if the localServer can't connect to the network.
     * @throws NullPointerException
     *             if {@code rootDir == null || predicate == null}.
     * @see #SourceNode(Archive, Predicate, PortNumberSet)
     */
    SourceNode(final Archive archive) throws IOException {
        this(archive, new InetSocketAddressSet());
    }

    /**
     * Constructs from the data archive, a specification of the locally-desired
     * data, and the port numbers for the localServer.
     * 
     * @param archive
     *            The data archive.
     * @param inetSockAddrSet
     *            The set of candidate Internet socket addresses for the server.
     * @throws IOException
     *             if the localServer can't connect to the network.
     * @throws NullPointerException
     *             if {@code archive == null || inetSockAddrSet == null}.
     * @see AbstractNode#AbstractNode(Archive, Predicate, InetSocketAddressSet)
     */
    SourceNode(final Archive archive, final InetSocketAddressSet inetSockAddrSet)
            throws IOException {
        super(archive, Predicate.NOTHING, inetSockAddrSet);
    }

    @Override
    Server createServer(final ClearingHouse clearingHouse,
            final InetSocketAddressSet inetSockAddrSet) throws SocketException,
            IOException {
        return new SourceServer(clearingHouse, inetSockAddrSet);
    }

    /**
     * Executes this instance. Never returns normally.
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
        try {
            taskManager.submit(localServer);
            taskManager.submit(fileWatcher);
            final Future<Void> future = taskManager.take();
            if (!future.isCancelled()) {
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
            }
        }
        finally {
            executorService.shutdownNow();
            Thread.currentThread().setName(origThreadName);
        }
        return null;
    }

    /**
     * Waits until this instance is running.
     * 
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    void waitUntilRunning() throws InterruptedException {
        localServer.waitUntilRunning();
        fileWatcher.waitUntilRunning();
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
        path = getArchive().getHiddenPath(path);
        Files.createDirectories(path.getParent());
        final SeekableByteChannel channel = Files.newByteChannel(path,
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
        return new PubFile(getArchive(), path);
    }

    @Override
    int getClientCount() {
        return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "SourceNode [archive=" + getArchive() + "]";
    }
}
