/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.prefs.Preferences;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.NotThreadSafe;
import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;

/**
 * An archive of files.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class Archive {
    /**
     * Tracker-specific administrative files that are distributed via the
     * network.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    static final class DistributedTrackerFiles {
        /**
         * Distributes tracker-specific files via the network.
         */
        private class Distributor extends Thread {
            /**
             * The last time an attempt was made to update the files in the
             * archive.
             */
            private ArchiveTime prevUpdateTime = ArchiveTime.BEGINNING_OF_TIME;

            @Override
            public void run() {
                for (;;) {
                    /*
                     * Prevent corruption of distributed files by serializing
                     * access and ensuring that the time-difference between old
                     * and new files is greater than or equal to the temporal
                     * resolution of the archive.
                     */
                    try {
                        final FilterServerMap topology = topologyLock.take();
                        final ArchiveTime now = new ArchiveTime();

                        if (prevUpdateTime.compareTo(now) >= 0) {
                            logger.debug(
                                    "Topology-file not distributed because it's not sufficiently new: {}",
                                    topologyArchivePath);
                        }
                        else {
                            try {
                                archive.save(topologyArchivePath, topology);
                            }
                            catch (final FileAlreadyExistsException e) {
                                logger.error(
                                        "The topology file was created by another thread!",
                                        e);
                            }
                            catch (final IOException e) {
                                logger.error("Couldn't save network topology",
                                        e);
                            }
                            prevUpdateTime = new ArchiveTime(); // ensures later
                        }
                    }
                    catch (final InterruptedException e) {
                        logger.error(
                                "This thread shouldn't have been interrupted",
                                e);
                    }
                    catch (final Throwable t) {
                        logger.error("Unexpected error", t);
                    }
                }
            }
        }

        /**
         * The data archive.
         */
        private final Archive                     archive;
        /**
         * The archive-pathname of the network topology file.
         */
        private final ArchivePath                 topologyArchivePath;
        /**
         * The absolute pathname of the network topology file.
         */
        private final Path                        topologyAbsolutePath;
        /**
         * The archive-pathname of the Internet address for reporting
         * unavailable servers.
         */
        private final ArchivePath                 reportingAddressArchivePath;
        /**
         * The object-lock for distributing the topology. NB: This is a
         * single-element, discarding queue rather than a Hoare monitor.
         */
        private final ObjectLock<FilterServerMap> topologyLock      = new ObjectLock<FilterServerMap>();
        /**
         * Distributes tracker-specific files via the network.
         */
        @GuardedBy("this")
        private final Thread                      distributor       = new Distributor();
        /**
         * The time when the network topology file in the archive was last
         * updated via the network.
         */
        @GuardedBy("this")
        private ArchiveTime                       networkUpdateTime = ArchiveTime.BEGINNING_OF_TIME;
        /**
         * The network topology obtained via the network.
         */
        @GuardedBy("this")
        private FilterServerMap                   topologyFromNetwork;

        /**
         * Constructs from the data archive and the address of the source-node's
         * server.
         * 
         * @param archive
         *            The data archive.
         * @param trackerAddress
         *            The address of the tracker.
         * @throws NullPointerException
         *             if {@code archive == null}.
         * @throws NullPointerException
         *             if {@code trackerAddress == null}.
         */
        DistributedTrackerFiles(final Archive archive,
                final InetSocketAddress trackerAddress) {
            this.archive = archive;
            final String packagePath = getClass().getPackage().getName();
            String packageName = packagePath.substring(packagePath
                    .lastIndexOf('.') + 1);
            packageName = packageName.toUpperCase();
            final ArchivePath trackerPath = archive.getAdminDir().resolve(
                    new ArchivePath(Paths.get(trackerAddress.getHostString()
                            + "-" + trackerAddress.getPort())));
            topologyArchivePath = trackerPath.resolve("topology");
            topologyAbsolutePath = archive.resolve(topologyArchivePath);
            reportingAddressArchivePath = trackerPath
                    .resolve("reportingAddress");
        }

        /**
         * Returns the path in the archive of the distributed file that contains
         * tracker-specific network topology information.
         * 
         * @return the identifier of the distributed file that contains
         *         tracker-specific network topology information.
         */
        ArchivePath getTopologyArchivePath() {
            return topologyArchivePath;
        }

        /**
         * Returns the time of the last update of the tracker-specific network
         * topology. {@link #getTopology()} might modify this time. This method
         * should only be called by a subscriber.
         * 
         * @return the time of the last update of the tracker-specific network
         *         topology.
         * @throws IOException
         *             if an I/O error occurs.
         */
        synchronized ArchiveTime getTopologyArchiveTime() throws IOException {
            return networkUpdateTime;
        }

        /**
         * Returns the tracker-specific network topology information obtained
         * via the network. Might modify the value returned by
         * {@link #getTopologyArchiveTime()}. The actual object is returned --
         * not a copy. This method should only be called by a subscriber.
         * 
         * @return the tracker-specific network topology information.
         * @throws NoSuchFileException
         *             if the tracker-specific topology file doesn't exist in
         *             the archive.
         * @throws IOException
         *             if an I/O error occurs.
         */
        synchronized FilterServerMap getTopology() throws NoSuchFileException,
                IOException {
            final ArchiveTime updateTime = new ArchiveTime(topologyAbsolutePath);
            if (networkUpdateTime.compareTo(updateTime) < 0) {
                try {
                    /*
                     * If the data is updated before the time, then an older
                     * version might not be updated if a newer version arrives
                     * between those two actions.
                     */
                    topologyFromNetwork = (FilterServerMap) archive.restore(
                            topologyArchivePath, FilterServerMap.class);
                    networkUpdateTime = updateTime;
                }
                catch (final ClassNotFoundException e) {
                    throw (IOException) new IOException(
                            "Invalid filter/server map type").initCause(e);
                }
                catch (final ClassCastException e) {
                    throw (IOException) new IOException(
                            "Invalid filter/server map type").initCause(e);
                }
                catch (final FileNotFoundException e) {
                    throw new NoSuchFileException(e.getLocalizedMessage());
                }
            }
            return topologyFromNetwork;
        }

        /**
         * Distributes the network topology throughout the network by saving the
         * network topology object in a file that will be subsequently
         * distributed if sufficient time has elapsed since the distribution of
         * the previous topology. This method should only be called by a
         * publisher of data.
         * 
         * @param topology
         *            The network topology.
         * @throws NullPointerException
         *             if {@code topology == null}.
         */
        void distribute(final FilterServerMap topology) {
            if (topology == null) {
                throw new NullPointerException();
            }
            ensureDistributorStarted();
            topologyLock.put(topology);
        }

        /**
         * Ensures that the distributor thread is started for distributing
         * administrative files via the network.
         */
        private synchronized void ensureDistributorStarted() {
            if (!distributor.isAlive()) {
                distributor.setDaemon(true);
                distributor.start();
            }
        }

        /**
         * Distributes the Internet socket address for reporting server
         * unavailability.
         * 
         * @param reportingAddress
         *            The Internet socket address for reporting server
         *            unavailability
         * @throws FileSystemException
         *             if too many files are open
         * @throws IOException
         *             if an I/O error occurs.
         */
        void distribute(final InetSocketAddress reportingAddress)
                throws FileSystemException, IOException {
            archive.save(reportingAddressArchivePath, reportingAddress);
        }

        /**
         * Returns the data-filter that matches the filter/server map.
         * 
         * @return the data-filter that matches the filter/server map.
         */
        Filter getFilterServerMapFilter() {
            return Filter.getInstance(topologyArchivePath.toString());
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return "DistributedTrackerFiles [topologyLock=" + topologyLock
                    + ", networkUpdateTime=" + networkUpdateTime + "]";
        }
    }

    /**
     * Factory for obtaining an object to manage the distribution of
     * tracker-specific administrative files.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private final class DistributedTrackerFilesFactory {
        /**
         * The map from a tracker address to an instance of a
         * {@link DistributeTrackerFiles}.
         */
        private final ConcurrentMap<InetSocketAddress, DistributedTrackerFiles> instances = new ConcurrentHashMap<InetSocketAddress, DistributedTrackerFiles>();

        /**
         * Returns an object for managing the distribution of tracker-specific
         * administrative files.
         * 
         * @param trackerAddress
         *            The address of the tracker.
         * @return An object for managing the distribution of tracker-specific
         *         administrative files.
         */
        DistributedTrackerFiles getInstance(
                final InetSocketAddress trackerAddress) {
            DistributedTrackerFiles instance = instances.get(trackerAddress);
            if (instance == null) {
                instance = new DistributedTrackerFiles(Archive.this,
                        trackerAddress);
                final DistributedTrackerFiles prevInstance = instances
                        .putIfAbsent(trackerAddress, instance);
                if (prevInstance != null) {
                    instance = prevInstance;
                }
            }
            return instance;
        }
    }

    /**
     * Watches for newly-created files in the file-tree.
     * 
     * Instances are thread-compatible but not thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @NotThreadSafe
    private final class FileWatcher {
        /**
         * Directory watch service.
         */
        private final WatchService        watchService;
        /**
         * Map from watch-key to pathname.
         */
        private final Map<WatchKey, Path> dirs = new HashMap<WatchKey, Path>();
        /**
         * Map from pathname to watch-key.
         */
        private final Map<Path, WatchKey> keys = new HashMap<Path, WatchKey>();
        /**
         * The associated local server.
         */
        private final Server              server;

        /**
         * Constructs from the local server to notify about new files. Doesn't
         * return.
         * 
         * @param server
         *            The local server to notify about changes to the archive.
         * @throws IOException
         *             if an I/O error occurs.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         * @throws NullPointerException
         *             if {@code server == null}.
         */
        FileWatcher(final Server server) throws IOException,
                InterruptedException {
            final String origThreadName = Thread.currentThread().getName();
            Thread.currentThread().setName(toString());
            this.server = server;
            try {
                if (null == server) {
                    throw new NullPointerException();
                }
                watchService = rootDir.getFileSystem().newWatchService();
                try {
                    registerAll(rootDir);
                    for (;;) {
                        final WatchKey key = watchService.take();
                        for (final WatchEvent<?> event : key.pollEvents()) {
                            final WatchEvent.Kind<?> kind = event.kind();
                            if (kind == StandardWatchEventKinds.OVERFLOW) {
                                logger.error(
                                        "Couldn't keep-up watching file-tree rooted at \"{}\"",
                                        rootDir);
                            }
                            else {
                                final Path name = (Path) event.context();
                                Path path = dirs.get(key);
                                path = path.resolve(name);
                                if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                                    try {
                                        newFile(path);
                                    }
                                    catch (final NoSuchFileException e) {
                                        // The file was just deleted
                                        logger.debug(
                                                "New file was just deleted: {}",
                                                path);
                                    }
                                    catch (final IOException e) {
                                        logger.error("Error with new file "
                                                + path, e);
                                    }
                                }
                                else if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                                    try {
                                        removedFile(path);
                                    }
                                    catch (final IOException e) {
                                        logger.error(
                                                "Error with removed file \""
                                                        + path + "\"", e);
                                    }
                                }
                            }
                        }
                        if (!key.reset()) {
                            final Path dir = dirs.remove(key);
                            if (null != dir) {
                                keys.remove(dir);
                            }
                        }
                    }
                }
                finally {
                    watchService.close();
                }
            }
            finally {
                Thread.currentThread().setName(origThreadName);
            }
        }

        /**
         * Handles the creation of a new file.
         * 
         * @param path
         *            The pathname of the new file.
         * @throws NoSuchFileException
         *             if the file doesn't exist
         * @throws IOException
         *             if an I/O error occurs.
         */
        private void newFile(final Path path) throws NoSuchFileException,
                IOException {
            final BasicFileAttributes attributes = Files.readAttributes(path,
                    BasicFileAttributes.class);
            if (attributes.isDirectory()) {
                registerAll(path);
                walkDirectory(path, new FilePieceSpecSetConsumer() {
                    @Override
                    public void consume(final FilePieceSpecSet spec) {
                        server.newData(spec);
                    }
                }, Filter.EVERYTHING);
            }
            else if (attributes.isRegularFile()) {
                ArchiveTime.adjustTime(path);
                final FileInfo fileInfo;
                final ArchivePath archivePath = new ArchivePath(path, rootDir);
                final FileId fileId = new FileId(archivePath, new ArchiveTime(
                        attributes));
                if (archivePath.startsWith(adminDir)) {
                    // Indefinite time-to-live
                    fileInfo = new FileInfo(fileId, attributes.size(),
                            PIECE_SIZE, -1);
                }
                else {
                    // Default time-to-live
                    fileInfo = new FileInfo(fileId, attributes.size(),
                            PIECE_SIZE);
                }
                server.newData(FilePieceSpecSet.newInstance(fileInfo, true));
            }
        }

        /**
         * Handles the removal of a file. If and only if the file is not a
         * distributed, administrative file, then the server is called to send a
         * removal-notice to all connected peers.
         * 
         * @param path
         *            The pathname of the removed file.
         * @throws IOException
         *             if an I/O error occurs.
         */
        private void removedFile(final Path path) throws IOException {
            /*
             * Distributed administrative files must not be deleted at remote
             * sites.
             */
            final ArchivePath archivePath = new ArchivePath(path, rootDir);
            if (!archivePath.startsWith(adminDir)) {
                final WatchKey k = keys.remove(path);
                if (null != k) {
                    dirs.remove(k);
                    k.cancel();
                }
                final FileId fileId = new FileId(archivePath,
                        ArchiveTime.BEGINNING_OF_TIME);
                server.removed(fileId);
            }
        }

        /**
         * Registers a directory.
         * 
         * @param dir
         *            Pathname of the directory.
         * @throws IOException
         *             if an I/O error occurs.
         */
        private void register(final Path dir) throws IOException {
            final WatchKey key = dir.register(watchService,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_DELETE);
            dirs.put(key, dir);
            keys.put(dir, key);
        }

        /**
         * Registers a directory and recursively registers all non-hidden
         * sub-directories.
         * 
         * @param dir
         *            Pathname of the directory.
         * @throws IOException
         *             if an I/O error occurs.
         */
        private void registerAll(final Path dir) throws IOException {
            final EnumSet<FileVisitOption> opts = EnumSet
                    .of(FileVisitOption.FOLLOW_LINKS);
            Files.walkFileTree(dir, opts, Integer.MAX_VALUE,
                    new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult preVisitDirectory(
                                final Path dir,
                                final BasicFileAttributes attributes) {
                            if (ArchiveFile.isHidden(rootDir, dir)) {
                                return FileVisitResult.SKIP_SUBTREE;
                            }
                            try {
                                register(dir);
                            }
                            catch (final IOException e) {
                                throw new IOError(e);
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    });
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return "FileWatcher [rootDir=" + rootDir + "]";
        }
    }

    /**
     * A data-file that resides on a disk.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    static abstract class ArchiveFile {
        /**
         * The absolute pathname of the file actually being accessed
         */
        @GuardedBy("lock")
        protected Path              path;
        /**
         * The I/O channel for the file.
         */
        @GuardedBy("lock")
        protected RandomAccessFile  randomFile;
        /**
         * Whether or not the file is complete (i.e., has all its data).
         */
        @GuardedBy("lock")
        protected boolean           isVisible;
        /**
         * The logical pathname of the archive-file
         */
        protected final ArchivePath archivePath;
        /**
         * The absolute pathname of the root-directory of the archive
         */
        protected final Path        rootDir;

        /**
         * Constructs from references to an existing archive file. The
         * archive-file is returned in an unlocked state.
         * 
         * @param rootDir
         *            The absolute pathname of the root-directory of the archive
         * @param archivePath
         *            Pathname of the archive file
         * @throws NullPointerException
         *             if {@code rootDir == null}
         * @throws NullPointerException
         *             if {@code archivePath == null}
         * @throws IllegalArgumentException
         *             if the pathname of the root-directory of the archive
         *             isn't absolute
         */
        ArchiveFile(final Path rootDir, final ArchivePath archivePath) {
            if (!rootDir.isAbsolute()) {
                throw new IllegalArgumentException();
            }
            this.rootDir = rootDir;
            if (archivePath == null) {
                throw new NullPointerException();
            }
            this.archivePath = archivePath;
        }

        /**
         * Closes this instance if necessary. Reveals a complete but hidden
         * file. Idempotent.
         * 
         * @throws IOException
         *             if an I/O error occurs.
         */
        protected abstract void close() throws IOException;

        /**
         * Deletes the archive-file. Closes it first if necessary.
         * 
         * @throws IOException
         *             if an I/O error occurs.
         */
        protected abstract void deleteIfExists() throws IOException;

        /**
         * Returns the absolute pathname of the hidden form of a pathname.
         * 
         * @param rootDir
         *            Absolute pathname of the root-directory of the archive
         * @param archivePath
         *            The archive pathname
         * @return The absolute pathname of the hidden form of the given
         *         pathname
         */
        protected static Path hide(final Path rootDir,
                final ArchivePath archivePath) {
            Path path = archivePath.getPath();
            path = HIDDEN_DIR.resolve(path);
            return rootDir.resolve(path);
        }

        /**
         * Returns the absolute pathname of the visible form of a hidden
         * pathname.
         * 
         * @param rootDir
         *            Absolute pathname of the root-directory of the archive
         * @param archivePath
         *            The archive pathname.
         * @return The absolute pathname of the visible form of the given
         *         pathname
         * @throws IllegalArgumentException
         *             if {@code path} isn't hidden.
         */
        protected static Path reveal(final Path rootDir,
                final ArchivePath archivePath) {
            final Path path = archivePath.getPath();
            return rootDir.resolve(path);
        }

        /**
         * Indicates whether or not a file or directory is hidden.
         * 
         * @param rootDir
         *            Absolute pathname of the root-directory of the archive
         * @param path
         *            Pathname of the file or directory in question. May be
         *            absolute or relative to the root-directory.
         * @return {@code true} if and only if the file or directory is hidden.
         * @throws NullPointerException
         *             if {@code path == null}.
         */
        static boolean isHidden(final Path rootDir, Path path) {
            if (path.isAbsolute()) {
                path = rootDir.relativize(path);
            }
            return (null == path)
                    ? false
                    : path.startsWith(HIDDEN_DIR);
        }
    }

    /**
     * An archive-file that presents a bulk (i.e., not piecewise) interface.
     * Such archive-files are made visible when they are closed.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private static final class BulkArchiveFile extends ArchiveFile {
        /**
         * Constructs from file information.
         * 
         * @param rootDir
         *            Absolute pathname of the root-directory of the archive
         * @param archivePath
         *            Pathname of the archive-file
         * @param readonly
         *            Whether the file will only be read
         * @param manager
         *            The manager for this instance
         * @throws NullPointerException
         *             if {@code rootDir == null}
         * @throws NullPointerException
         *             if {@code archivePath == null}
         * @throws IllegalArgumentException
         *             if {@code !rootDir.isAbsolute}
         * @throws FileNotFoundException
         *             if the file doesn't exist
         * @throws FileSystemException
         *             if too many files are open
         * @throws IOException
         *             if an I/O error occurs
         */
        BulkArchiveFile(final Path rootDir, final ArchivePath archivePath,
                final boolean readonly) throws FileNotFoundException,
                FileSystemException, IOException {
            super(rootDir, archivePath);
            if (readonly) {
                initFromVisibleFile();
            }
            else {
                initByCreatingHiddenFile();
            }
        }

        /**
         * Initializes this instance from a complete, visible file.
         * 
         * @throws FileSystemException
         *             if too many files are open
         * @throws FileNotFoundException
         *             if the file doesn't exist
         * @throws IOException
         *             if an I/O error occurs
         */
        private synchronized void initFromVisibleFile()
                throws FileSystemException, FileNotFoundException, IOException {
            path = reveal(rootDir, archivePath);
            isVisible = true;
            randomFile = new RandomAccessFile(path.toFile(), "r");
        }

        /**
         * Initializes this instance by creating an empty, hidden file.
         * 
         * @throws FileSystemException
         *             if too many files are open
         * @throws IOException
         *             if an I/O error occurs.
         */
        private synchronized void initByCreatingHiddenFile()
                throws FileSystemException, IOException {
            path = hide(rootDir, archivePath);
            Files.createDirectories(path.getParent());
            isVisible = false;
            randomFile = new RandomAccessFile(path.toFile(), "rw");
        }

        /**
         * Returns the archive-time of an existing file in the archive.
         * 
         * @param rootDir
         *            Absolute pathname of the root-directory of the archive
         * @param archivePath
         *            Pathname of the archive-file
         * @return The archive-time of the given archive-file
         * @throws FileSystemException
         *             if too many files are open
         * @throws NoSuchFileException
         *             if the file doesn't exist
         * @throws IOException
         *             if an I/O exception occurs
         */
        static ArchiveTime getTime(final Path rootDir,
                final ArchivePath archivePath) throws FileSystemException,
                NoSuchFileException, IOException {
            return new ArchiveTime(archivePath.getAbsolutePath(rootDir));
        }

        /**
         * Transfers bytes from a readable byte channel to the archive-file.
         * 
         * @param channel
         *            The channel from which to read the bytes to be written to
         *            the archive file.
         * @throws IOException
         *             if an I/O error occurs
         */
        synchronized void transferFrom(final ReadableByteChannel channel)
                throws IOException {
            randomFile.getChannel().transferFrom(channel, 0, Long.MAX_VALUE);
        }

        /**
         * Present the archive-file as an input stream. The user should not
         * close the returned input stream.
         * 
         * @return The entire archive-file as an input stream
         * @throws IllegalStateException
         *             if the archive-file is incomplete
         * @throws IOException
         *             if an I/O error occurs
         */
        synchronized InputStream asInputStream() throws IOException {
            if (!isVisible) {
                throw new IllegalStateException();
            }
            randomFile.seek(0);
            return Channels.newInputStream(randomFile.getChannel());
        }

        /**
         * Closes this instance. The archive-file is considered complete and is
         * made visible. Idempotent.
         * 
         * @throws IOException
         *             if an I/O error occurs.
         */
        @Override
        protected synchronized void close() throws IOException {
            if (randomFile != null) {
                randomFile.close();
                final Path newPath = reveal(rootDir, archivePath);
                for (;;) {
                    try {
                        Files.createDirectories(newPath.getParent());
                        try {
                            Files.move(path, newPath,
                                    StandardCopyOption.ATOMIC_MOVE,
                                    StandardCopyOption.REPLACE_EXISTING);
                            path = newPath;
                            logger.debug("Newly-visible file: {}", path);
                            break;
                        }
                        catch (final NoSuchFileException e) {
                            // A directory in the path was just
                            // deleted
                            logger.trace(
                                    "Directory in path just deleted by another thread: {}",
                                    newPath);
                        }
                    }
                    catch (final NoSuchFileException e) {
                        // A directory in the path was just deleted
                        logger.trace(
                                "Directory in path just deleted by another thread: {}",
                                newPath);
                    }
                }
                randomFile = null;
                isVisible = true;
            }
        }

        /**
         * Deletes the archive-file. Closes it first if necessary.
         * 
         * @throws IOException
         *             if an I/O error occurs.
         */
        @Override
        protected synchronized void deleteIfExists() throws IOException {
            try {
                close();
            }
            catch (final IOException ignored) {
            }
            Files.deleteIfExists(path);
        }

        /**
         * Deletes an archive-file.
         * 
         * @param rootDir
         *            Absolute pathname of the root-directory of the archive
         * @param archivePath
         *            Pathname of the archive-file.
         * @throws NullPointerException
         *             if {@code rootDir == null}
         * @throws FileSystemException
         *             if too many files are open
         * @throws IOException
         *             if an I/O error occurs
         */
        static void delete(final Path rootDir, final ArchivePath archivePath)
                throws IOException {
            /*
             * The following should work if renaming a file and deleting a file
             * are atomic.
             */
            // This should handle most files
            Path path = archivePath.getAbsolutePath(rootDir);
            if (!Files.deleteIfExists(path)) {
                // The file might still be hidden
                path = hide(rootDir, archivePath);
                if (!Files.deleteIfExists(path)) {
                    // The file might have just been renamed
                    path = reveal(rootDir, archivePath);
                    if (!Files.deleteIfExists(path)) {
                        logger.debug("File doesn't exist: {}", path);
                    }
                }
            }
            /*
             * Now delete empty ancestor directories up to, but not including,
             * the first non-empty directory or the root directory, whichever
             * comes first.
             */
            Path dir = null;
            try {
                for (dir = path.getParent(); dir != null
                        && !Files.isSameFile(dir, rootDir) && isEmpty(dir); dir = dir
                        .getParent()) {
                    try {
                        if (!Files.deleteIfExists(dir)) {
                            // The ancestor directory, "dir", just
                            // ceased to exist
                            logger.debug(
                                    "Directory was just deleted by another thread: {}",
                                    dir);
                            break;
                        }
                    }
                    catch (final DirectoryNotEmptyException ignored) {
                        // A file must have just been added.
                        logger.debug(
                                "Not deleting directory because it was just added-to by another thread: {}",
                                dir);
                    }
                }
            }
            catch (final NoSuchFileException ignored) {
                // The ancestor directory, "dir", just ceased to exist
                logger.debug(
                        "Directory was just deleted by another thread: {}", dir);
            }
        }
    }

    /**
     * Indicates a corrupt hidden file.
     */
    private static class BadHiddenFileException extends IOException {
        /**
         * Serial version identifier
         */
        private static final long serialVersionUID = 1L;
        /**
         * Absolute pathname of the corrupt hidden file.
         */
        private final Path        path;

        BadHiddenFileException(final Path path, final String msg) {
            super(msg);
            this.path = path;
        }

        @Override
        public String toString() {
            return getMessage() + " [path=" + path + "]";
        }
    }

    /**
     * An archive-file that presents a segmented (i.e., piecewise) interface.
     * Such archive-files can be closed while they are still incomplete and are
     * made visible only when complete.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    private static final class SegmentedArchiveFile extends ArchiveFile {
        /**
         * The set of existing pieces.
         */
        @GuardedBy("lock")
        private FiniteBitSet        indexes;
        /**
         * Information on the data-product.
         */
        @GuardedBy("lock")
        private FileInfo            fileInfo;
        /**
         * Reentrant lock for this instance
         */
        private final ReentrantLock lock = new ReentrantLock();

        /**
         * Constructs from the pathname of the root-directory.
         * 
         * @param rootDir
         *            Absolute pathname of the root-directory of the archive
         * @param fileInfo
         *            Information on the file
         * @throws NullPointerException
         *             if {@code rootDir == null}
         * @throws NullPointerException
         *             if {@code fileInfo == null}
         * @throws IllegalArgumentException
         *             if {@code !rootDir.isAbsolute()}
         */
        private SegmentedArchiveFile(final Path rootDir, final FileInfo fileInfo) {
            super(rootDir, fileInfo.getPath());
        }

        /**
         * Returns a new instance in an unlocked state or {@code null} if the
         * file only needs to be read and doesn't exist.
         * 
         * @param rootDir
         *            Absolute pathname of the root-directory of the archive
         * @param fileInfo
         *            Information on the file. The file-information of the
         *            returned instance will be based on the actual archive-file
         *            and might differ from the expected file-information.
         * @param readonly
         *            Whether or not the file only needs to be read
         * @return A new instance or {@code null}
         * @throws NullPointerException
         *             if {@code rootDir == null}
         * @throws NullPointerException
         *             if {@code fileInfo == null}
         * @throws IllegalArgumentException
         *             if {@code !rootDir.isAbsolute()}
         * @throws FileSystemException
         *             if too many files are open
         * @throws IOException
         *             if an I/O error occurs
         */
        static SegmentedArchiveFile newInstance(final Path rootDir,
                final FileInfo fileInfo, final boolean readonly)
                throws FileSystemException, IOException {
            final ArchivePath archivePath = fileInfo.getPath();
            final SegmentedArchiveFile file = new SegmentedArchiveFile(rootDir,
                    fileInfo);
            Path path = hide(rootDir, archivePath);
            if (Files.exists(path)) {
                // The hidden file exists
                try {
                    file.openHiddenFile(fileInfo);
                }
                catch (final FileNotFoundException e) {
                    logger.debug(
                            "Hidden file just deleted by another thread: {}",
                            path);
                    if (!readonly) {
                        logger.debug("Creating new hidden file.");
                        file.createHiddenFile(fileInfo);
                    }
                }
                catch (final BadHiddenFileException e) {
                    logger.debug("Deleting corrupt hidden file {}: {}", path,
                            e.toString());
                    try {
                        Files.delete(path);
                    }
                    catch (final IOException e2) {
                        logger.debug("Couldn't delete hidden file {}: {}",
                                path, e2.toString());
                    }
                    if (!readonly) {
                        logger.debug("Creating new hidden file");
                        file.createHiddenFile(fileInfo);
                    }
                }
            }
            else {
                // The hidden file doesn't exist
                path = reveal(rootDir, archivePath);
                if (!Files.exists(path)) {
                    // The visible file doesn't exist
                    if (!readonly) {
                        file.createHiddenFile(fileInfo);
                    }
                }
                else {
                    // The visible file exists
                    try {
                        file.openVisibleFile(fileInfo);
                    }
                    catch (final FileNotFoundException e) {
                        logger.debug(
                                "Visible file just deleted by another thread: {}",
                                path);
                        if (!readonly) {
                            logger.debug("Creating new hidden file");
                            file.createHiddenFile(fileInfo);
                        }
                    }
                }
            }
            return file.randomFile == null
                    ? null
                    : file;
        }

        /**
         * Initializes this instance from a complete, visible file.
         * 
         * @param template
         *            Template file-information. Used to set metadata that can't
         *            be determined from the actual file.
         * @throws FileSystemException
         *             if too many files are open
         * @throws FileNotFoundException
         *             if the file doesn't exist
         * @throws IOException
         *             if an I/O error occurs
         */
        private void openVisibleFile(final FileInfo template)
                throws FileSystemException, FileNotFoundException, IOException {
            lock();
            try {
                final ArchivePath archivePath = template.getPath();
                path = reveal(rootDir, archivePath);
                isVisible = true;
                final RandomAccessFile randomFile = new RandomAccessFile(
                        path.toFile(), "r");
                final FileId fileId = new FileId(archivePath, new ArchiveTime(
                        path));
                fileInfo = new FileInfo(fileId, randomFile.length(),
                        template.getPieceSize(), template.getTimeToLive());
                indexes = new CompleteBitSet(fileInfo.getPieceCount());
                this.randomFile = randomFile;
            }
            finally {
                unlock();
            }
        }

        /**
         * Initializes this instance by creating an empty, hidden file.
         * 
         * @param fileInfo
         *            Template file-information. Used to set file metadata.
         * @throws FileSystemException
         *             if too many files are open
         * @throws IOException
         *             if an I/O error occurs.
         */
        private void createHiddenFile(final FileInfo fileInfo)
                throws FileSystemException, IOException {
            lock();
            try {
                path = hide(rootDir, archivePath);
                this.fileInfo = fileInfo;
                isVisible = false;
                Files.createDirectories(path.getParent());
                indexes = new PartialBitSet(fileInfo.getPieceCount());
                randomFile = new RandomAccessFile(path.toFile(), "rw");
            }
            finally {
                unlock();
            }
        }

        /**
         * Initializes this instance from an incomplete, hidden file.
         * 
         * @param template
         *            File-information that the archive-file is expected to
         *            have. The subsequent file-information of this instance
         *            might be different.
         * @throws FileSystemException
         *             if too many files are open
         * @throws FileNotFoundException
         *             if the file doesn't exist
         * @throws BadHiddenFileException
         *             if the hidden file is corrupt
         * @throws IOException
         *             if an I/O error occurs
         */
        private void openHiddenFile(final FileInfo template)
                throws FileSystemException, FileNotFoundException,
                BadHiddenFileException, IOException {
            lock();
            try {
                path = hide(rootDir, archivePath);
                isVisible = false;
                boolean closeIt = true;
                final RandomAccessFile randomFile = new RandomAccessFile(
                        path.toFile(), "rw");
                try {
                    final long longSize = Long.SIZE / Byte.SIZE;
                    final long fileLength;

                    try {
                        fileLength = randomFile.length();
                    }
                    catch (final IOException e) {
                        throw (IOException) new IOException(
                                "Couldn't get length of file \"" + path + "\"")
                                .initCause(e);
                    }

                    long pos = fileLength - longSize;
                    if (pos < 0) {
                        throw new BadHiddenFileException(path,
                                "Hidden file too small to be valid");
                    }

                    try {
                        randomFile.seek(pos);
                    }
                    catch (final IOException e) {
                        throw (IOException) new IOException(
                                "Couldn't seek to byte " + pos + " in file \""
                                        + path + "\"").initCause(e);
                    }

                    try {
                        pos = randomFile.readLong();
                    }
                    catch (final IOException e) {
                        throw (IOException) new IOException("Couldn't read "
                                + "byte-offset of metadata in file \"" + path
                                + "\"").initCause(e);
                    }

                    if (pos < 0 || pos >= fileLength) {
                        throw new BadHiddenFileException(path,
                                "Invalid metadata offset: " + pos);
                    }

                    try {
                        randomFile.seek(pos);
                    }
                    catch (final IOException e) {
                        throw (IOException) new IOException(
                                "Couldn't seek to byte " + pos + " in file \""
                                        + path + "\"").initCause(e);
                    }

                    FileInputStream inputStream;
                    try {
                        inputStream = new FileInputStream(randomFile.getFD());
                    }
                    catch (final IOException e) {
                        throw (IOException) new IOException(
                                "Couldn't get file-descriptor for file \""
                                        + path + "\"").initCause(e);
                    }

                    try {
                        ObjectInputStream ois;
                        try {
                            ois = new ObjectInputStream(inputStream);
                        }
                        catch (final StreamCorruptedException e) {
                            throw (BadHiddenFileException) new BadHiddenFileException(
                                    path,
                                    "Couldn't get object-input-stream for file")
                                    .initCause(e);
                        }
                        catch (final IOException e) {
                            throw (IOException) new IOException(
                                    "Couldn't get object-input-stream for "
                                            + "file \"" + path + "\"")
                                    .initCause(e);
                        }

                        try {
                            fileInfo = (FileInfo) ois.readObject();
                            indexes = (FiniteBitSet) ois.readObject();
                            this.randomFile = randomFile;
                            closeIt = false;
                        }
                        catch (final ClassNotFoundException e) {
                            throw (BadHiddenFileException) new BadHiddenFileException(
                                    path, "Couldn't read file metadata")
                                    .initCause(e);
                        }
                        catch (final ObjectStreamException e) {
                            throw (BadHiddenFileException) new BadHiddenFileException(
                                    path, "Couldn't read file metadata")
                                    .initCause(e);
                        }
                        catch (final IOException e) {
                            throw (IOException) new IOException(
                                    "Couldn't read file metadata: " + path)
                                    .initCause(e);
                        }
                        finally {
                            if (closeIt) {
                                try {
                                    ois.close();
                                }
                                catch (final IOException ignored) {
                                }
                                closeIt = false;
                            }
                        }
                    }
                    finally {
                        if (closeIt) {
                            try {
                                inputStream.close();
                            }
                            catch (final IOException ignored) {
                            }
                            closeIt = false;
                        }
                    }
                }
                finally {
                    if (closeIt) {
                        try {
                            randomFile.close();
                        }
                        catch (final IOException ignored) {
                        }
                    }
                }
            }
            finally {
                unlock();
            }
        }

        /**
         * Returns the time associated with this instance.
         * 
         * @return the time associated with this instance.
         */
        ArchiveTime getTime() {
            lock();
            try {
                return fileInfo.getTime();
            }
            finally {
                unlock();
            }
        }

        /**
         * Returns the file's metadata.
         * 
         * @return the file's metadata.
         */
        FileInfo getFileInfo() {
            lock();
            try {
                return fileInfo;
            }
            finally {
                unlock();
            }
        }

        /**
         * Returns the file-identifier.
         * 
         * @return the file-identifier
         */
        FileId getFileId() {
            lock();
            try {
                return fileInfo.getFileId();
            }
            finally {
                unlock();
            }
        }

        /**
         * Writes a piece of data. If the data-piece completes the file, then
         * the file is moved from the hidden file-tree to the visible file-tree
         * in a manner that is robust in the face of removal of necessary
         * directories by another thread.
         * 
         * @param piece
         *            The piece of data.
         * @return {@code true} if and only if the file is complete, in which
         *         case the file is now visible.
         * @throws FileSystemException
         *             if too many files are open.
         * @throws NoSuchFileException
         *             if the file no longer exists.
         * @throws IOException
         *             if an I/O error occurs.
         * @throws NullPointerException
         *             if {@code piece == null}.
         */
        boolean putPiece(final Piece piece) throws FileSystemException,
                IOException {
            lock();
            try {
                final int index = piece.getIndex();
                if (!indexes.isSet(index)) {
                    randomFile.seek(piece.getOffset());
                    randomFile.write(piece.getData());
                    indexes = indexes.setBit(index);
                    if (indexes.areAllSet()) {
                        close();
                        assert isVisible;
                        openVisibleFile(fileInfo);
                    }
                }
                return isVisible;
            }
            finally {
                unlock();
            }
        }

        /**
         * Indicates if the archive-file contains a particular piece of data.
         * 
         * @param index
         *            Index of the piece of data.
         * @return {@code true} if and only if the archive-file contains the
         *         piece of data.
         * @throws IllegalArgumentException
         *             if the index is outside the valid range of indexes
         */
        boolean hasPiece(final int index) {
            lock();
            try {
                return indexes.isSet(index);
            }
            finally {
                unlock();
            }
        }

        /**
         * Returns a piece of data.
         * 
         * @param pieceSpec
         *            Information on the piece of data.
         * @throws FileSystemException
         *             if too many files are open.
         * @throws IOException
         *             if an I/O error occurs.
         */
        Piece getPiece(final PieceSpec pieceSpec) throws FileSystemException,
                IOException {
            lock();
            try {
                assert indexes.isSet(pieceSpec.getIndex());
                final byte[] data = new byte[pieceSpec.getSize()];
                randomFile.seek(pieceSpec.getOffset());
                final int nbytes = randomFile.read(data);
                assert nbytes == data.length;
                return new Piece(pieceSpec, data);
            }
            finally {
                unlock();
            }
        }

        /**
         * Closes this instance if necessary. If the file is complete, then it
         * is made visible. Idempotent.
         * 
         * @throws IOException
         *             if an I/O error occurs.
         */
        @Override
        protected void close() throws IOException {
            lock();
            try {
                if (randomFile != null) {
                    if (isVisible) {
                        randomFile.close();
                    }
                    else {
                        final long length = fileInfo.getSize();
                        if (!indexes.areAllSet()) {
                            randomFile.seek(length);
                            final OutputStream outputStream = new FileOutputStream(
                                    randomFile.getFD());
                            final ObjectOutputStream oos = new ObjectOutputStream(
                                    outputStream);
                            oos.writeObject(fileInfo);
                            oos.writeObject(indexes);
                            oos.writeLong(length);
                            oos.close();
                            fileInfo.getTime().setTime(path);
                        }
                        else {
                            randomFile.getChannel().truncate(length);
                            randomFile.close();
                            final Path newPath = reveal(rootDir, archivePath);
                            for (;;) {
                                try {
                                    Files.createDirectories(newPath.getParent());
                                    try {
                                        Files.move(
                                                path,
                                                newPath,
                                                StandardCopyOption.ATOMIC_MOVE,
                                                StandardCopyOption.REPLACE_EXISTING);
                                        path = newPath;
                                        isVisible = true;
                                        fileInfo.getTime().setTime(path);
                                        logger.debug("Complete file: {}",
                                                fileInfo);
                                        break;
                                    }
                                    catch (final NoSuchFileException e) {
                                        // A directory in the path was just
                                        // deleted
                                        logger.trace(
                                                "Directory in path just deleted by another thread: {}",
                                                newPath);
                                    }
                                }
                                catch (final NoSuchFileException e) {
                                    // A directory in the path was just deleted
                                    logger.trace(
                                            "Directory in path just deleted by another thread: {}",
                                            newPath);
                                }
                            }
                        }
                    }
                    randomFile = null;
                }
            }
            finally {
                unlock();
            }
        }

        /**
         * Locks this instance.
         */
        void lock() {
            lock.lock();
        }

        /**
         * Unlocks this instance.
         */
        void unlock() {
            lock.unlock();
        }

        /**
         * Deletes the archive-file. Closes it first if necessary.
         * 
         * @throws IOException
         *             if an I/O error occurs.
         */
        @Override
        protected void deleteIfExists() throws IOException {
            lock();
            try {
                try {
                    close();
                }
                catch (final IOException ignored) {
                }
                Files.deleteIfExists(path);
            }
            finally {
                unlock();
            }
        }

        /**
         * Ensures that a file that matches a file-identifier doesn't exist.
         * 
         * @param rootDir
         *            Absolute pathname of the root-directory of the archive
         * @param fileId
         *            Identifier of the file
         * @throws IllegalArgumentException
         *             if {@code !rootDir.isAbsolute()}
         * @throws FileSystemException
         *             if too many files are open
         * @throws IOException
         *             if an I/O error occurs
         */
        static void deleteIfExists(final Path rootDir, final FileId fileId)
                throws IOException {
            final Path path = fileId.getAbsolutePath(rootDir);
            try {
                if (fileId.equals(FileId.getInstance(path, rootDir))) {
                    Files.deleteIfExists(path);
                }
            }
            catch (final NoSuchFileException ignored) {
            }
        }
    }

    /**
     * Manages a collection of archive-files.
     * 
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @ThreadSafe
    final class ArchiveFileManager {
        private final class ArchiveFileMap extends
                LinkedHashMap<ArchivePath, SegmentedArchiveFile> {
            /**
             * The serial version identifier.
             */
            private static final long serialVersionUID = 1L;

            /**
             * The maximum number of open files.
             */
            private final int         maxNumOpenFiles;

            private ArchiveFileMap(final int maxNumOpenFiles) {
                super(maxNumOpenFiles, 0.75f, true);
                this.maxNumOpenFiles = maxNumOpenFiles;
            }

            @Override
            protected boolean removeEldestEntry(
                    final Map.Entry<ArchivePath, SegmentedArchiveFile> entry) {
                if (size() > maxNumOpenFiles) {
                    final ArchiveFile archiveFile = entry.getValue();
                    try {
                        archiveFile.close();
                    }
                    catch (final NoSuchFileException e) {
                        logger.error("File deleted by another thread: \"{}\"",
                                archiveFile);
                    }
                    catch (final IOException e) {
                        logger.error("Couldn't close file \"" + archiveFile
                                + "\"", e);
                    }
                    return true;
                }
                return false;
            }
        }

        /**
         * The set of open, segmented archive-files.
         */
        @GuardedBy("itself")
        private final ArchiveFileMap openSegmentedFiles;

        /**
         * Constructs from the maximum number of open files.
         * 
         * @param maxNumOpenFiles
         *            Maximum number of open files
         * @throws IllegalArgumentException
         *             if {@code maxNumOpenFiles <= 0}
         */
        ArchiveFileManager(final int maxNumOpenFiles) {
            if (maxNumOpenFiles <= 0) {
                throw new IllegalArgumentException();
            }
            openSegmentedFiles = new ArchiveFileMap(maxNumOpenFiles);
        }

        /**
         * Returns a bulk archive-file for reading or {@code null} if the
         * archive-file doesn't exist.
         * 
         * @param archivePath
         *            Pathname of the archive file.
         * @return The relevant bulk archive-file or {@code null} if the
         *         archive-file doesn't exist.
         * @throws FileNotFoundException
         *             if the archive-file doesn't exist
         * @throws FileSystemException
         *             if too many files are open. This exception will be thrown
         *             only after all open files in this collection have been
         *             closed.
         * @throws IOException
         *             if an I/O error occurs
         */
        BulkArchiveFile getForReading(final ArchivePath archivePath)
                throws FileNotFoundException, FileSystemException, IOException {
            final BulkArchiveFile file = new BulkArchiveFile(rootDir,
                    archivePath, true);
            return file;
        }

        /**
         * Returns a bulk archive-file for writing.
         * 
         * @param archivePath
         *            Pathname of the archive file.
         * @return The bulk archive-file.
         * @throws FileSystemException
         *             if too many files are open. This exception will be thrown
         *             only after all open, segmented archive-files in this
         *             collection have been closed.
         * @throws IOException
         *             if an I/O error occurs
         */
        BulkArchiveFile getForWriting(final ArchivePath archivePath)
                throws FileNotFoundException, FileSystemException, IOException {
            final BulkArchiveFile file = new BulkArchiveFile(rootDir,
                    archivePath, false);
            return file;
        }

        /**
         * Returns a locked, segmented archive-file for writing or {@code null}.
         * 
         * @param fileInfo
         *            Information on the target file
         * @param readonly
         *            if the file will only be read
         * @return The segmented archive-file corresponding to the given
         *         file-information or {@code  null} if {@code readonly} is true
         *         and the specified file doesn't exist or if {@code readonly}
         *         is false and the archive-file is newer than the target file.
         * @throws FileInfoMismatchException
         *             if the archive-file exists but its file-information is
         *             incompatible with the given file-information
         * @throws FileSystemException
         *             if too many files are open. This exception will be thrown
         *             only after all open files in this collection have been
         *             closed.
         * @throws IOException
         *             if an I/O error occurs
         */
        public SegmentedArchiveFile get(final FileInfo fileInfo,
                final boolean readonly) throws FileInfoMismatchException,
                FileSystemException, IOException {
            SegmentedArchiveFile file;
            final ArchivePath archivePath = fileInfo.getPath();
            synchronized (openSegmentedFiles) {
                for (;;) {
                    file = openSegmentedFiles.get(archivePath);
                    if (file == null) {
                        file = getArchiveFile(fileInfo, readonly);
                        if (file == null) {
                            return null;
                        }
                        final ArchiveFile prevFile = openSegmentedFiles.put(
                                archivePath, file);
                        assert prevFile == null;
                    }
                    /*
                     * Vet the archive-file's file-information against the
                     * expected file-information
                     */
                    final FileInfo archiveFileInfo = file.getFileInfo();
                    if (fileInfo.equals(archiveFileInfo)) {
                        file.lock();
                        return file;
                    }

                    final int cmp = fileInfo.getTime()
                            .compareTo(file.getTime());
                    if (readonly || cmp < 0) {
                        /*
                         * The file-informations differ and the archive-file
                         * will only be read or is newer than the specified
                         * file.
                         */
                        return null;
                    }
                    if (cmp > 0) {
                        /*
                         * The file-informations differ and the archive-file
                         * will be written and is older than the specified file.
                         */
                        try {
                            file.deleteIfExists();
                        }
                        finally {
                            openSegmentedFiles.remove(archivePath);
                        }
                    }
                    else {
                        /*
                         * The file-information of the archive-file is
                         * incompatible with the given file-information.
                         */
                        throw new FileInfoMismatchException(fileInfo,
                                archiveFileInfo);
                    }
                }
            }
        }

        /**
         * Returns a newly-created, segmented archive-file or {@code null} if
         * the file only needs to be read and doesn't exist.
         * 
         * @param fileInfo
         *            Information on the file
         * @param readonly
         *            Whether or not the file only needs to be read
         * @return The newly-created archive-file or {@code null}.
         * @throws FileSystemException
         *             if too many files are open. This exception will be thrown
         *             only after all open files in this collection have been
         *             closed.
         * @throws IOException
         *             if an I/O error occurs
         */
        private SegmentedArchiveFile getArchiveFile(final FileInfo fileInfo,
                final boolean readonly) throws FileSystemException, IOException {
            assert Thread.holdsLock(openSegmentedFiles);
            for (;;) {
                try {
                    return SegmentedArchiveFile.newInstance(rootDir, fileInfo,
                            readonly);
                }
                catch (final FileSystemException e) {
                    // Too many open files
                    if (removeLru() == null) {
                        throw e;
                    }
                }
            }
        }

        /**
         * Removes the least-recently-used (LRU), segmented archive-file from
         * the map: gets the LRU, segmented archive-file, ensures that it's
         * closed, and removes it from the map.
         * 
         * @return The removed, segmented archive-file or {@code null} if the
         *         map is empty.
         * @throws IOException
         *             if an I/O error occurs.
         */
        private SegmentedArchiveFile removeLru() throws IOException {
            assert Thread.holdsLock(openSegmentedFiles);
            final SegmentedArchiveFile file;
            final Iterator<Map.Entry<ArchivePath, SegmentedArchiveFile>> iter = openSegmentedFiles
                    .entrySet().iterator();
            if (!iter.hasNext()) {
                return null;
            }
            file = iter.next().getValue();
            file.close();
            iter.remove();
            return file;
        }

        /**
         * Returns the archive-time of a file in the archive or {@code null} if
         * the file doesn't yet exist.
         * 
         * @param archivePath
         *            Pathname of the file in the archive
         * @return The archive-time of the given file or {@code null}
         * @throws FileSystemException
         *             if too many files are open. This will only be thrown
         *             after all open segmented archive-files have been closed.
         * @throws IOException
         *             if an I/O error occurs
         */
        ArchiveTime getTime(final ArchivePath archivePath)
                throws FileSystemException, IOException {
            synchronized (openSegmentedFiles) {
                final SegmentedArchiveFile file = openSegmentedFiles
                        .get(archivePath);
                if (file != null) {
                    return file.getTime();
                }
                for (;;) {
                    try {
                        return BulkArchiveFile.getTime(rootDir, archivePath);
                    }
                    catch (final FileSystemException e) {
                        if (removeLru() == null) {
                            throw e;
                        }
                    }
                }
            }
        }

        /**
         * Deletes a bulk archive-file.
         * 
         * @param archivePath
         *            Pathname of the archive-file.
         * @throws IOException
         *             if an I/O error occurs
         */
        void delete(final ArchivePath archivePath) throws IOException {
            for (;;) {
                try {
                    BulkArchiveFile.delete(rootDir, archivePath);
                    break;
                }
                catch (final FileSystemException e) {
                    if (removeLru() == null) {
                        throw e;
                    }
                }
            }
        }

        /**
         * Deletes an archive-file if it exists.
         * 
         * @param fileId
         *            File-identifier of the archive-file. All attributes must
         *            match.
         * @throws FileSystemException
         *             if too many files are open. This will only be thrown
         *             after all open segmented archive-files have been closed.
         * @throws IOException
         *             if an I/O error occurs
         */
        void deleteIfExists(final FileId fileId) throws FileSystemException,
                IOException {
            final ArchivePath path = fileId.getPath();
            synchronized (openSegmentedFiles) {
                final SegmentedArchiveFile file = openSegmentedFiles.get(path);
                if (file != null) {
                    if (fileId.equals(file.getFileId())) {
                        file.deleteIfExists();
                        openSegmentedFiles.remove(path);
                    }
                }
                else {
                    for (;;) {
                        try {
                            SegmentedArchiveFile
                                    .deleteIfExists(rootDir, fileId);
                            break;
                        }
                        catch (final FileSystemException e) {
                            if (removeLru() == null) {
                                throw e;
                            }
                        }
                    }
                }
            }
        }

        /**
         * Closes this instance, releasing all segmented archive-file resources.
         * 
         * @throws IOException
         */
        void closeAll() throws IOException {
            synchronized (openSegmentedFiles) {
                for (final Iterator<Map.Entry<ArchivePath, SegmentedArchiveFile>> iter = openSegmentedFiles
                        .entrySet().iterator(); iter.hasNext();) {
                    iter.next().getValue().close();
                    iter.remove();
                }
            }
        }
    }

    /**
     * The logger for this package.
     */
    private static final Logger                  logger                         = Util.getLogger();
    /**
     * The name of the hidden directory that will be ignored for the most part.
     */
    private static final Path                    HIDDEN_DIR                     = Paths.get(".sruth");
    /**
     * The canonical size, in bytes, of a piece of data (131072).
     */
    private static final int                     PIECE_SIZE                     = 0x20000;
    /**
     * The maximum number of open files.
     */
    private static final int                     ACTIVE_FILE_CACHE_SIZE;
    private static final int                     ACTIVE_FILE_CACHE_SIZE_DEFAULT = 512;
    private static final String                  ACTIVE_FILE_CACHE_SIZE_KEY     = "active file cache size";
    /**
     * The pathname of the root of the file-tree.
     */
    private final Path                           rootDir;
    /**
     * The file-deleter.
     */
    private final DelayedPathActionQueue         delayedPathActionQueue;
    /**
     * The listeners for data-products.
     */
    @GuardedBy("itself")
    private final List<DataProductListener>      dataProductListeners           = new LinkedList<DataProductListener>();
    /**
     * The factory for obtaining an object for managing the distribution of
     * tracker-specific administrative files.
     */
    private final DistributedTrackerFilesFactory distributedTrackerFilesFactory = new DistributedTrackerFilesFactory();
    /**
     * The archive pathname of the administrative-files directory.
     */
    private final ArchivePath                    adminDir                       = new ArchivePath(
                                                                                        Util.PACKAGE_NAME);
    /**
     * The manager of the archive-files.
     */
    private final ArchiveFileManager             archiveFileManager;

    static {
        final Preferences prefs = Preferences.userNodeForPackage(Archive.class);
        ACTIVE_FILE_CACHE_SIZE = prefs.getInt(ACTIVE_FILE_CACHE_SIZE_KEY,
                ACTIVE_FILE_CACHE_SIZE_DEFAULT);
        if (ACTIVE_FILE_CACHE_SIZE <= 0) {
            throw new IllegalArgumentException("Invalid user-preference \""
                    + ACTIVE_FILE_CACHE_SIZE_KEY + "\": "
                    + ACTIVE_FILE_CACHE_SIZE);
        }
    }

    /**
     * Constructs from the pathname of the root of the file-tree.
     * 
     * @param rootDir
     *            The pathname of the root of the file-tree.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code rootDir == null}.
     */
    Archive(final String rootDir) throws IOException {
        this(Paths.get(rootDir));
    }

    /**
     * Constructs from the pathname of the root of the file-tree. The maximum
     * number of open files will be determined by the user-preference
     * {@value #ACTIVE_FILE_CACHE_SIZE_KEY} (default
     * {@value #ACTIVE_FILE_CACHE_SIZE_DEFAULT}).
     * 
     * @param rootDir
     *            The pathname of the root of the file-tree.
     * @param maxNumOpenFiles
     *            The maximum number of open files.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code rootDir == null}.
     */
    Archive(final Path rootDir) throws IOException {
        this(rootDir, ACTIVE_FILE_CACHE_SIZE);
    }

    /**
     * Constructs from the pathname of the root of the file-tree and the maximum
     * number of open files to have.
     * 
     * @param rootDir
     *            The pathname of the root of the file-tree.
     * @param maxNumOpenFiles
     *            The maximum number of open files.
     * @throws IllegalArgumentException
     *             if {@code maxNumOpenFiles <= 0}
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code rootDir == null}.
     */
    Archive(final Path rootDir, final int maxNumOpenFiles) throws IOException {
        if (null == rootDir) {
            throw new NullPointerException();
        }
        if (maxNumOpenFiles <= 0) {
            throw new IllegalArgumentException(
                    "Invalid maximum number of open file: " + maxNumOpenFiles);
        }
        final Path hiddenDir = rootDir.resolve(HIDDEN_DIR);
        final Path fileDeletionQueuePath = hiddenDir
                .resolve("fileDeletionQueue");
        Files.createDirectories(hiddenDir);
        purgeHiddenDir(hiddenDir, fileDeletionQueuePath);
        /*
         * According to the Java 7 tutorial, the following is valid:
         * 
         * Attributes.setAttribute(hiddenDir, "dos:hidden", true);
         * 
         * but the given method doesn't exist in reality. Hence, the following:
         */
        try {
            final Boolean hidden = (Boolean) Files.getAttribute(hiddenDir,
                    "dos:hidden", LinkOption.NOFOLLOW_LINKS);
            if (null != hidden && !hidden) {
                // The file-system is DOS and the hidden directory isn't hidden
                Files.setAttribute(hiddenDir, "dos:hidden", Boolean.TRUE,
                        LinkOption.NOFOLLOW_LINKS);
            }
        }
        catch (final FileSystemException ignored) {
            // The file-system isn't DOS
        }
        this.rootDir = rootDir;
        delayedPathActionQueue = new DelayedPathActionQueue(rootDir,
                new PathDelayQueue(fileDeletionQueuePath),
                new DelayedPathActionQueue.Action() {
                    @Override
                    void act(final Path path) throws IOException {
                        archiveFileManager
                                .delete(new ArchivePath(path, rootDir));
                    }

                    @Override
                    public String toString() {
                        return "DELETE";
                    }
                });
        archiveFileManager = new ArchiveFileManager(maxNumOpenFiles);
    }

    /**
     * Purges the hidden directory of all files that shouldn't exist at the
     * start of a session (i.e., cleans-up from a previous session).
     * 
     * @param hiddenDir
     *            Pathname of the hidden directory
     * @param keepPath
     *            Pathname of the only file to keep.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private static void purgeHiddenDir(final Path hiddenDir, final Path keepPath)
            throws IOException {
        final EnumSet<FileVisitOption> opts = EnumSet
                .of(FileVisitOption.FOLLOW_LINKS);
        Files.walkFileTree(hiddenDir, opts, Integer.MAX_VALUE,
                new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(final Path path,
                            final BasicFileAttributes attributes)
                            throws IOException {
                        if (!path.equals(keepPath)) {
                            try {
                                Files.delete(path);
                            }
                            catch (final IOException e) {
                                logger.error("Couldn't purge file: " + path, e);
                                throw e;
                            }
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(final Path dir,
                            final IOException e) throws IOException {
                        if (e != null) {
                            throw e;
                        }
                        if (!dir.equals(hiddenDir)) {
                            try {
                                Files.delete(dir);
                            }
                            catch (final IOException e2) {
                                logger.error(
                                        "Couldn't purge directory: " + dir, e2);
                                throw e2;
                            }
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
    }

    /**
     * Returns the pathname of the root directory of the file-tree.
     * 
     * @return Pathname of the root directory of the file-tree.
     */
    Path getRootDir() {
        return rootDir;
    }

    /**
     * Returns the pathname of the administrative-files directory relative to
     * this archive.
     * 
     * @return the pathname of the administrative-files directory relative to
     *         this archive.
     */
    ArchivePath getAdminDir() {
        return adminDir;
    }

    /**
     * Returns the archive pathname corresponding to an absolute pathname.
     * 
     * @param path
     *            The absolute pathname to be made relative to this archive.
     * @return The archive pathname corresponding to the absolute pathname.
     * @throws IllegalArgumentException
     *             if {@code !path.isAbsolute()}.
     * @throws IllegalArgumentException
     *             if {@code path} can't be made relative to this archive (i.e.,
     *             {@code path} lies outside this archive).
     * @throws NullPointerException
     *             if {@code path == null}.
     * @see #resolve(ArchivePath)
     */
    ArchivePath relativize(final Path path) {
        return new ArchivePath(path, rootDir);
    }

    /**
     * Returns the absolute pathname corresponding to an archive pathname.
     * 
     * @param path
     *            The archive pathname.
     * @return The corresponding absolute pathname.
     * @see #relativize(Path)
     */
    Path resolve(final ArchivePath path) {
        return path.getAbsolutePath(rootDir);
    }

    /**
     * Returns an object for managing the distribution of tracker-specific,
     * administrative files.
     * 
     * @param trackerAddress
     *            The address of the tracker.
     * @return An object for managing the distribution of the tracker-specific
     *         administrative files.
     */
    DistributedTrackerFiles getDistributedTrackerFiles(
            final InetSocketAddress trackerAddress) {
        return distributedTrackerFilesFactory.getInstance(trackerAddress);
    }

    /**
     * Indicates whether or not a piece of data exists.
     * 
     * @param dir
     *            Pathname of the output directory.
     * @param pieceSpec
     *            Specification of the piece of data.
     * @return {@code true} if and only if the piece of data exists.
     * @throws FileSystemException
     *             if too many files are open.
     * @throws IOException
     *             if an I/O error occurs.
     */
    boolean exists(final PieceSpec pieceSpec) throws FileSystemException,
            IOException {
        SegmentedArchiveFile file;
        try {
            file = archiveFileManager.get(pieceSpec.getFileInfo(), true);
        }
        catch (final FileInfoMismatchException e) {
            logger.debug("Incompatible file-information: {}", e.toString());
            return false;
        }

        if (file == null) {
            return false;
        }
        try {
            return file.hasPiece(pieceSpec.getIndex());
        }
        finally {
            file.unlock();
        }
    }

    /**
     * Returns a piece of data.
     * 
     * @param pieceSpec
     *            Information on the piece of data.
     * @return The piece of data or {@code null} if the piece is unavailable.
     * @throws FileSystemException
     *             if too many files are open.
     * @throws IOException
     *             if an I/O error occurred.
     */
    Piece getPiece(final PieceSpec pieceSpec) throws FileSystemException,
            IOException {
        final SegmentedArchiveFile file;
        try {
            file = archiveFileManager.get(pieceSpec.getFileInfo(), true);
        }
        catch (final FileInfoMismatchException e) {
            logger.warn("Incompatible file-information", e);
            return null;
        }

        if (file == null) {
            return null;
        }
        try {
            return file.getPiece(pieceSpec);
        }
        finally {
            file.unlock();
        }
    }

    /**
     * Writes a piece of data. If a newer version of the file exists, then the
     * data isn't written.
     * 
     * @param piece
     *            Piece of data to be written.
     * @return {@code true} if and only if the file is now complete.
     * @throws FileSystemException
     *             if too many files are open.
     * @throws NoSuchFileException
     *             if the destination file was deleted.
     * @throws FileInfoMismatchException
     *             if the file-information of the archive-file is inconsistent
     *             with that of the given piece
     * @throws IOException
     *             if an I/O error occurred.
     * @throws NullPointerException
     *             if {@code piece == null}.
     */
    boolean putPiece(final Piece piece) throws FileSystemException,
            NoSuchFileException, FileInfoMismatchException, IOException {
        final FileInfo fileInfo = piece.getFileInfo();
        final SegmentedArchiveFile file = archiveFileManager.get(fileInfo,
                false);
        if (file == null) {
            // A newer version of the file exists.
            logger.trace("Newer file version exists: {}", fileInfo);
            return false;
        }
        try {
            final boolean isComplete = file.putPiece(piece);
            if (isComplete) {
                final int timeToLive = piece.getTimeToLive();
                if (timeToLive >= 0) {
                    delayedPathActionQueue.actUponEventurally(
                            fileInfo.getAbsolutePath(rootDir),
                            1000 * timeToLive);
                }
                synchronized (dataProductListeners) {
                    for (final DataProductListener listener : dataProductListeners) {
                        final DataProduct product = new DataProduct(rootDir,
                                fileInfo);
                        listener.process(product);
                    }
                }
            }
            return isComplete;
        }
        finally {
            file.unlock();
        }
    }

    /**
     * Saves an object in the archive. The file will have an indefinite
     * time-to-live.
     * 
     * @param archivePath
     *            The pathname for the object in the archive.
     * @param serializable
     *            The object to be saved in the file.
     * @throws FileSystemException
     *             if too many files are open
     * @throws IOException
     *             if an I/O error occurs.
     */
    private void save(final ArchivePath archivePath,
            final Serializable serializable) throws FileSystemException,
            IOException {
        final byte[] bytes = Util.serialize(serializable);
        final InputStream inputStream = new ByteArrayInputStream(bytes);
        final ReadableByteChannel channel = Channels.newChannel(inputStream);
        save(archivePath, channel);
    }

    /**
     * Saves data in the archive. The file will have an indefinite time-to-live.
     * 
     * @param path
     *            The pathname for the data in the archive.
     * @param byteBuf
     *            The data. All of it will be used.
     * @throws FileInfoMismatchException
     *             if the given file information is inconsistent with an
     *             existing archive file
     * @throws IOException
     *             if an I/O error occurs.
     */
    void save(final ArchivePath path, final ByteBuffer byteBuf)
            throws FileAlreadyExistsException, IOException,
            FileInfoMismatchException {
        final byte[] bytes = byteBuf.array();
        final InputStream inputStream = new ByteArrayInputStream(bytes);
        final ReadableByteChannel channel = Channels.newChannel(inputStream);
        save(path, channel);
    }

    /**
     * Creates an archive-file. The file will have an indefinite time-to-live.
     * 
     * @param path
     *            The pathname for the data in the archive.
     * @param channel
     *            The channel from which to read the data for the file.
     * @throws FileSystemException
     *             if too many files are open
     * @throws IOException
     *             if an I/O error occurs, including insufficient data in the
     *             channel
     */
    void save(final ArchivePath path, final ReadableByteChannel channel)
            throws FileSystemException, IOException {
        save(path, channel, -1);
    }

    /**
     * Creates an archive-file.
     * 
     * @param archivePath
     *            The pathname of the archive-file
     * @param channel
     *            The channel from which to read the data for the file.
     * @param timeToLive
     *            The lifetime of the archive-file in seconds. A negative value
     *            means indefinitely.
     * @throws FileSystemException
     *             if too many files are open
     * @throws IOException
     *             if an I/O error occurs
     */
    void save(final ArchivePath archivePath, final ReadableByteChannel channel,
            final int timeToLive) throws FileSystemException, IOException {
        final BulkArchiveFile file = archiveFileManager
                .getForWriting(archivePath);
        boolean success = false;
        try {
            file.transferFrom(channel);
            if (timeToLive >= 0) {
                delayedPathActionQueue
                        .actUponEventurally(
                                archivePath.getAbsolutePath(rootDir),
                                1000 * timeToLive);
            }
            success = true;
        }
        finally {
            file.close();
            if (!success) {
                archiveFileManager.delete(archivePath);
            }
        }
    }

    /**
     * Restores an object from a file.
     * 
     * @param archivePath
     *            Pathname of the file in the archive.
     * @param type
     *            Expected type of the restored object.
     * @throws FileNotFoundException
     *             if the file doesn't exist.
     * @throws StreamCorruptedException
     *             if the file is corrupt.
     * @throws ClassNotFoundException
     *             if the type of the restored object is unknown.
     * @throws ClassCastException
     *             if the object isn't the expected type.
     * @throws IOException
     *             if an I/O error occurs.
     */
    Object restore(final ArchivePath archivePath, final Class<?> type)
            throws FileNotFoundException, StreamCorruptedException,
            ClassNotFoundException, IOException {
        final BulkArchiveFile file = archiveFileManager
                .getForReading(archivePath);
        boolean success = false;
        try {
            final InputStream in = file.asInputStream();
            // TODO: buffer the input stream?
            final ObjectInputStream ois = new ObjectInputStream(in);
            try {
                final Object obj = ois.readObject();
                if (!type.isInstance(obj)) {
                    throw new ClassCastException("expected=" + type
                            + ", actual=" + obj.getClass());
                }
                success = true;
                return obj;
            }
            catch (final StreamCorruptedException e) {
                throw (StreamCorruptedException) new StreamCorruptedException(
                        "Corrupted file: " + archivePath).initCause(e);
            }
        }
        finally {
            file.close();
            if (!success) {
                archiveFileManager.delete(archivePath);
            }
        }
    }

    /**
     * Returns the archive-time of an archive-file.
     * 
     * @param archivePath
     *            The pathname of the file in the archive.
     * @return The archive-time of the archive-file.
     * @throws FileNotFoundException
     *             if the archive-file doesn't exist.
     * @throws IOException
     *             if an I/O error occurs.
     */
    ArchiveTime getArchiveTime(final ArchivePath archivePath)
            throws FileNotFoundException, IOException {
        return archiveFileManager.getTime(archivePath);
    }

    /**
     * Removes the archive-file that matches a file identifier.
     * 
     * @param fileId
     *            Identifier of the file to be removed. All attributes must be
     *            matched.
     * @throws FileSystemException
     *             if too many files are open
     * @throws IOException
     *             if an I/O error occurs.
     */
    void remove(final FileId fileId) throws FileSystemException, IOException {
        archiveFileManager.deleteIfExists(fileId);
    }

    /**
     * Watches the archive for new files and removed files and directories.
     * Ignores hidden directories. Doesn't return.
     * 
     * @param server
     *            The local server.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     */
    void watchArchive(final Server server) throws IOException,
            InterruptedException {
        new FileWatcher(server);
    }

    /**
     * Returns the hidden form of a pathname.
     * 
     * @param path
     *            The pathname whose hidden form is to be returned.
     * @return The hidden form of {@code path}.
     */
    Path getHiddenPath(final Path path) {
        return ArchiveFile.hide(rootDir, new ArchivePath(path, rootDir));
    }

    /**
     * Returns the visible form of a hidden pathname
     * 
     * @param path
     *            The hidden pathname whose visible form is to be returned.
     * @return The visible form of {@code path}.
     */
    Path getVisiblePath(final Path path) {
        return ArchiveFile.reveal(rootDir, new ArchivePath(path, rootDir));
    }

    /**
     * Returns the absolute pathname of the hidden form of a visible pathname
     * that's relative to the root of the archive.
     * 
     * @param path
     *            The visible pathname relative to the root of the archive.
     * @return The corresponding absolute, hidden pathname.
     */
    Path getHiddenAbsolutePath(final Path path) {
        return rootDir.resolve(getHiddenPath(path));
    }

    /**
     * Recursively visits all the file-based data-specifications in a directory
     * that match a selection criteria. Doesn't visit files in hidden
     * directories.
     * 
     * @param root
     *            The directory to recursively walk.
     * @param consumer
     *            The consumer of file-based data-specifications.
     * @param filter
     *            The selection criteria.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private void walkDirectory(final Path root,
            final FilePieceSpecSetConsumer consumer, final Filter filter)
            throws IOException {
        final EnumSet<FileVisitOption> opts = EnumSet
                .of(FileVisitOption.FOLLOW_LINKS);
        Files.walkFileTree(root, opts, Integer.MAX_VALUE,
                new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult preVisitDirectory(final Path dir,
                            final BasicFileAttributes attributes) {
                        return ArchiveFile.isHidden(rootDir, dir)
                                ? FileVisitResult.SKIP_SUBTREE
                                : FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(final Path path,
                            final BasicFileAttributes attributes) {
                        if (attributes.isRegularFile()) {
                            final ArchiveTime archiveTime = new ArchiveTime(
                                    attributes);
                            try {
                                archiveTime.setTime(path);
                                final ArchivePath archivePath = new ArchivePath(
                                        path, rootDir);
                                if (filter.matches(archivePath)) {
                                    final FileId fileId = new FileId(
                                            archivePath, archiveTime);
                                    final FileInfo fileInfo;
                                    if (archivePath.startsWith(adminDir)) {
                                        // Indefinite time-to-live
                                        fileInfo = new FileInfo(fileId,
                                                attributes.size(), PIECE_SIZE,
                                                -1);
                                    }
                                    else {
                                        // Default time-to-live
                                        fileInfo = new FileInfo(fileId,
                                                attributes.size(), PIECE_SIZE);
                                    }
                                    final FilePieceSpecSet specSet = FilePieceSpecSet
                                            .newInstance(fileInfo, true);
                                    logger.trace("Path={}", archivePath);
                                    consumer.consume(specSet);
                                }
                            }
                            catch (final IOException e) {
                                logger.error(
                                        "Couldn't adjust time of file {}: {}",
                                        path, e.toString());
                            }
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
    }

    /**
     * Visits all the file-based data-specifications in the archive that match a
     * selection criteria. Doesn't visit files in hidden directories.
     * 
     * @param consumer
     *            The consumer of file-based data-specifications.
     * @param filter
     *            The selection criteria.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void walkArchive(final FilePieceSpecSetConsumer consumer,
            final Filter filter) throws IOException {
        walkDirectory(rootDir, consumer, filter);
    }

    /**
     * Adds a listener for data-products.
     * 
     * @param dataProductListener
     *            The listener to be added.
     */
    void addDataProductListener(final DataProductListener dataProductListener) {
        synchronized (dataProductListeners) {
            dataProductListeners.add(dataProductListener);
        }
    }

    /**
     * Removes a listener for data-products.
     * 
     * @param dataProductListener
     *            The listener to be removed.
     */
    void removeDataProductListener(final DataProductListener dataProductListener) {
        synchronized (dataProductListeners) {
            dataProductListeners.remove(dataProductListener);
        }
    }

    /**
     * Indicates if a directory is empty.
     * 
     * @param dir
     *            Pathname of the directory in question.
     * @return {@code true} if and only if the directory is empty.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private static boolean isEmpty(final Path dir) throws IOException {
        final DirectoryStream<Path> stream = Files.newDirectoryStream(dir);
        try {
            // See {@link FileAccessTest#testEmptyDirectory()}
            return !stream.iterator().hasNext();
        }
        finally {
            stream.close();
        }
    }

    /**
     * Closes this instance. Closes all open files and stops the file-deleter.
     * 
     * @throws IOException
     *             if an I/O error occurs.
     * @throws InterruptedException
     *             if the current thread is interrupted
     */
    void close() throws IOException, InterruptedException {
        try {
            delayedPathActionQueue.stop();
        }
        finally {
            archiveFileManager.closeAll();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Archive [rootDir=" + rootDir + "]";
    }
}
