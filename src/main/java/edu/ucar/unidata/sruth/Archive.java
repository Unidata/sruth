/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
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
import java.nio.file.StandardOpenOption;
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
                                archive.save(topologyArchivePath, topology, -1);
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
            final ArchivePath path = new ArchivePath(Paths.get(
                    trackerAddress.getHostString() + ":"
                            + trackerAddress.getPort()).resolve(
                    "FilterServerMap"));
            topologyArchivePath = archive.getAdminDir().resolve(path);
            topologyAbsolutePath = archive.resolve(topologyArchivePath);
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
         *            The local server to notify about new files.
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
         * Registers a directory and recursively registers all sub-directories.
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
                            if (pathname.isHidden(dir)) {
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
     * Utility class for hiding and revealing files based on pathnames.
     * <p>
     * Instances are thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    final class Pathname {
        /**
         * Indicates whether or not a file or directory is hidden.
         * 
         * @param path
         *            Pathname of the file or directory in question. May be
         *            absolute or relative to the root-directory.
         * @return {@code true} if and only if the file or directory is hidden.
         * @throws NullPointerException
         *             if {@code path == null}.
         */
        boolean isHidden(Path path) {
            if (path.isAbsolute()) {
                path = rootDir.relativize(path);
            }
            return (null == path)
                    ? false
                    : path.startsWith(HIDDEN_DIR);
        }

        /**
         * Returns the hidden form of a visible pathname.
         * 
         * @param path
         *            Pathname of the file to be hidden. May be absolute or
         *            relative to the root-directory.
         * @return The hidden pathname. If {@code path} is absolute, then the
         *         returned path is absolute; otherwise, it is relative to the
         *         root-directory.
         * @throws NullPointerException
         *             if {@code path == null}.
         */
        Path hide(Path path) {
            if (path.isAbsolute()) {
                path = rootDir.relativize(path);
                path = HIDDEN_DIR.resolve(path);
                return rootDir.resolve(path);
            }
            return HIDDEN_DIR.resolve(path);
        }

        /**
         * Returns the visible form of a hidden pathname.
         * 
         * @param path
         *            The hidden pathname.
         * @return The visible pathname.
         * @throws IllegalArgumentException
         *             if {@code path} isn't hidden.
         */
        Path reveal(Path path) {
            if (path.isAbsolute()) {
                path = rootDir.relativize(path);
                path = path.subpath(1, path.getNameCount());
                return rootDir.resolve(path);
            }
            return path.subpath(1, path.getNameCount());
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
    private final class DiskFile {
        /**
         * The set of existing pieces.
         */
        @GuardedBy("lock")
        private FiniteBitSet        indexes;
        /**
         * The pathname of the file.
         */
        @GuardedBy("lock")
        private Path                path;
        /**
         * The I/O channel for the file.
         */
        @GuardedBy("lock")
        private RandomAccessFile    randomFile;
        /**
         * Whether or not the file is hidden.
         */
        @GuardedBy("lock")
        private boolean             isComplete;
        /**
         * The lock for this instance.
         */
        private final ReentrantLock lock = new ReentrantLock();
        /**
         * Information on the data-product.
         */
        private FileInfo            fileInfo;

        /**
         * Constructs from information on the file. If the file exists, then its
         * metadata is set from the file; otherwise, its metadata is set from
         * the given file-information.
         * 
         * @param fileInfo
         *            Information on the file.
         * @throws FileNotFoundException
         *             if the file doesn't exist and can't be created.
         * @throws FileSystemException
         *             if too many files are open.
         */
        DiskFile(final FileInfo fileInfo) throws FileSystemException,
                IOException {
            lock.lock();
            try {
                /*
                 * First, try to open a complete file in read-only mode
                 */
                Path path = fileInfo.getAbsolutePath(rootDir);
                if (Files.exists(path)) {
                    // The complete file exists.
                    try {
                        initFromVisibleFile(path, fileInfo);
                    }
                    catch (final NoSuchFileException e) {
                        // The file was just deleted by another thread.
                        logger.warn("Complete archive file {} was just "
                                + "deleted by another thread. Continuing with "
                                + "new hidden file.", path);
                    }
                }

                if (randomFile == null) {
                    /*
                     * The complete file must not exist. Try opening the hidden
                     * version for writing.
                     */
                    path = pathname.hide(path);

                    if (Files.exists(path)) {
                        try {
                            initFromHiddenFile(path, fileInfo);
                        }
                        catch (final FileNotFoundException e) {
                            // The file was just deleted by another thread.
                            logger.warn("Hidden archive file \"{}\" was just "
                                    + "deleted by another thread. "
                                    + "Continuing with new hidden file.", path);
                        }
                        catch (final IOException e) {
                            logger.error(
                                    "Couldn't access hidden archive file \""
                                            + path
                                            + "\". Continuing with new hidden file.",
                                    e);
                            try {
                                Files.deleteIfExists(path);
                            }
                            catch (final IOException ignored) {
                            }
                        }
                    }

                    if (randomFile == null) {
                        initByCreatingHiddenFile(path, fileInfo);
                    }
                }
            }
            catch (final FileSystemException e) {
                throw e;
            }
            catch (final IOException e) {
                try {
                    close();
                }
                catch (final IOException ignored) {
                }
                throw e;
            }
            finally {
                lock.unlock();
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
         * Initializes this instance from a complete, visible file.
         * 
         * @param path
         *            The pathname of the file.
         * @param template
         *            Template file-information. Used to set metadata that can't
         *            be determined from the actual file.
         * @throws IOException
         *             if an I/O error occurs
         */
        private void initFromVisibleFile(final Path path,
                final FileInfo template) throws IOException {
            this.path = path;
            isComplete = true;
            randomFile = new RandomAccessFile(path.toFile(), "r");
            final FileId fileId = new FileId(template.getPath(),
                    new ArchiveTime(path));
            fileInfo = new FileInfo(fileId, randomFile.length(),
                    template.getPieceSize(), template.getTimeToLive());
            indexes = new CompleteBitSet(fileInfo.getPieceCount());
        }

        /**
         * Initializes this instance by creating an empty, hidden file.
         * 
         * @param path
         *            The pathname of the file
         * @param template
         *            Template file-information. Used to set file metadata.
         * @throws IOException
         *             if an I/O error occurs.
         */
        private void initByCreatingHiddenFile(final Path path,
                final FileInfo template) throws IOException {
            this.path = path;
            isComplete = false;
            Files.createDirectories(path.getParent());
            randomFile = new RandomAccessFile(path.toFile(), "rw");
            fileInfo = template;
            fileInfo.getTime().setTime(path);
            indexes = new PartialBitSet(fileInfo.getPieceCount());
        }

        /**
         * Initializes this instance from an incomplete, hidden file.
         * 
         * @param path
         *            The pathname of the file
         * @param template
         *            Template file-information. Used to set metadata that can't
         *            be determined from the actual file.
         * @throws FileNotFoundException
         *             if the file doesn't exist.
         * @throws IOException
         *             if an I/O error occurs.
         */
        private void initFromHiddenFile(final Path path, final FileInfo template)
                throws FileNotFoundException, IOException {
            this.path = path;
            isComplete = false;
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
                            "Couldn't get file-descriptor for file \"" + path
                                    + "\"").initCause(e);
                }

                try {
                    ObjectInputStream ois;
                    try {
                        ois = new ObjectInputStream(inputStream);
                    }
                    catch (final IOException e) {
                        throw (IOException) new IOException(
                                "Couldn't get object-input-stream for "
                                        + "file \"" + path + "\"").initCause(e);
                    }

                    try {
                        fileInfo = (FileInfo) ois.readObject();
                        indexes = (FiniteBitSet) ois.readObject();
                        this.randomFile = randomFile;
                        closeIt = false;
                    }
                    catch (final ClassNotFoundException e) {
                        throw (IOException) new IOException(
                                "Couldn't read file metadata: " + path)
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

        /**
         * Opens the channel to the file if necessary. Idempotent.
         * 
         * @throws FileSystemException
         *             if too many files are open.
         * @throws NoSuchFileException
         *             if the file doesn't exist.
         * @throws IOException
         *             if an I/O error occurs.
         */
        private void ensureOpen() throws FileSystemException,
                NoSuchFileException, IOException {
            if (randomFile == null) {
                if (isComplete) {
                    randomFile = new RandomAccessFile(path.toFile(), "r");
                }
                else {
                    randomFile = new RandomAccessFile(path.toFile(), "rw");
                    if (randomFile.length() == 0) {
                        randomFile.close();
                        Files.deleteIfExists(path);
                        throw new NoSuchFileException("Empty hidden file: "
                                + path);
                    }
                }
            }
        }

        /**
         * Closes this instance if necessary. Idempotent.
         * 
         * @throws IOException
         *             if an I/O error occurs.
         */
        void close() throws IOException {
            lock.lock();
            try {
                if (randomFile != null) {
                    if (isComplete) {
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
                        }
                        else {
                            randomFile.getChannel().truncate(length);
                            final Path newPath = pathname.reveal(path);
                            for (;;) {
                                try {
                                    Files.createDirectories(newPath.getParent());
                                    try {
                                        Files.move(path, newPath,
                                                StandardCopyOption.ATOMIC_MOVE);
                                        path = newPath;
                                        isComplete = true;
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
                            randomFile.close();
                        }
                        fileInfo.getTime().setTime(path);
                    }
                    randomFile = null;
                }
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Returns the time associated with this instance.
         * 
         * @return the time associated with this instance.
         */
        ArchiveTime getTime() {
            lock.lock();
            try {
                return fileInfo.getTime();
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Returns the file's metadata.
         * 
         * @return the file's metadata.
         */
        FileInfo getFileInfo() {
            lock.lock();
            try {
                return fileInfo;
            }
            finally {
                lock.unlock();
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
         *         case the file is closed.
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
            lock.lock();
            try {
                final int index = piece.getIndex();
                if (!indexes.isSet(index)) {
                    ensureOpen();
                    randomFile.seek(piece.getOffset());
                    randomFile.write(piece.getData());
                    indexes = indexes.setBit(index);
                    if (indexes.areAllSet()) {
                        close();
                        final int timeToLive = piece.getTimeToLive();
                        if (timeToLive >= 0) {
                            delayedPathActionQueue.actUponEventurally(path,
                                    1000 * timeToLive);
                        }
                    }
                }
                return isComplete;
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Indicates if the disk file contains a particular piece of data.
         * 
         * @param index
         *            Index of the piece of data.
         * @return {@code true} if and only if the disk file contains the piece
         *         of data.
         */
        boolean hasPiece(final int index) {
            lock.lock();
            try {
                return indexes.isSet(index);
            }
            finally {
                lock.unlock();
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
            lock.lock();
            try {
                final byte[] data = new byte[pieceSpec.getSize()];
                ensureOpen();
                randomFile.seek(pieceSpec.getOffset());
                randomFile.read(data);
                return new Piece(pieceSpec, data);
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Deletes the disk-file. Closes it first if necessary.
         * 
         * @throws IOException
         *             if an I/O error occurs.
         */
        void delete() throws IOException {
            lock.lock();
            try {
                try {
                    close();
                }
                catch (final IOException ignored) {
                }
                Files.delete(path);
            }
            finally {
                lock.unlock();
            }
        }
    }

    private final class DiskFileMap extends
            LinkedHashMap<ArchivePath, DiskFile> {
        /**
         * The serial version identifier.
         */
        private static final long serialVersionUID = 1L;

        private DiskFileMap() {
            super(16, 0.75f, true);
        }

        @Override
        protected boolean removeEldestEntry(
                final Map.Entry<ArchivePath, DiskFile> entry) {
            if (size() > activeFileCacheSize) {
                final DiskFile diskFile = entry.getValue();
                try {
                    diskFile.close();
                }
                catch (final NoSuchFileException e) {
                    logger.error("File deleted by another thread: \"{}\"",
                            diskFile);
                }
                catch (final IOException e) {
                    logger.error("Couldn't close file \"" + diskFile + "\"", e);
                }
                return true;
            }
            return false;
        }
    }

    /**
     * The logger for this class.
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
    private final int                            activeFileCacheSize;
    /**
     * The pathname utility for hidden pathnames.
     */
    private final Pathname                       pathname                       = new Pathname();
    /**
     * The set of active disk files.
     */
    @GuardedBy("itself")
    private final DiskFileMap                    diskFiles                      = new DiskFileMap();
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
        activeFileCacheSize = maxNumOpenFiles;
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
                        delete(path);
                    }

                    @Override
                    public String toString() {
                        return "DELETE";
                    }
                });
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
     * Deletes a file. Iteratively deletes parent directories if they are now
     * empty.
     * 
     * @param path
     *            Pathname of the file to be deleted.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private void delete(final Path path) throws IOException {
        /*
         * The following should work if renaming a file and deleting a file are
         * atomic.
         */
        try {
            // This should handle most files
            Files.delete(path);
        }
        catch (final NoSuchFileException e) {
            // The file might still be hidden
            final Path hiddenPath = getHiddenPath(path);
            try {
                Files.delete(hiddenPath);
            }
            catch (final NoSuchFileException e2) {
                // The file might have just been renamed
                try {
                    Files.delete(path);
                }
                catch (final NoSuchFileException e3) {
                    logger.info("File doesn't exist: {}", path);
                }
            }
        }
        Path dir = null;
        try {
            for (dir = path.getParent(); dir != null
                    && !Files.isSameFile(dir, rootDir) && isEmpty(dir); dir = dir
                    .getParent()) {
                try {
                    Files.delete(dir);
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
            // A parent directory, "dir", has ceased to exist
            logger.debug("Directory was just deleted by another thread: {}",
                    dir);
        }
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
     * Returns the on-disk file associated with a file. Creates the file if it
     * doesn't already exist. The file is returned in a locked state. If a
     * version of the file exists with an earlier associated time, then it is
     * first deleted. If a version exists with a later archive-time, then
     * {@code NULL} is returned.
     * <p>
     * The number of active disk-files is limited by the smaller of the
     * {@value #ACTIVE_FILE_CACHE_SIZE_KEY} user-preference (default
     * {@value #ACTIVE_FILE_CACHE_SIZE_DEFAULT}) and the maximum number of open
     * files allowed by the operating system.
     * 
     * @param fileInfo
     *            Information on the file
     * @return The associated, locked file or {@code null} if a newer version of
     *         the file exists.
     * @throws NullPointerException
     *             if {@code fileInfo == null}.
     * @throws FileSystemException
     *             if too many files are open. The disk-file map is now empty
     *             and all files are closed.
     * @throws FileInfoMismatchException
     *             if the extant file has the same archive-time as the given
     *             file-information but the file-informations otherwise differ.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private DiskFile getDiskFile(final FileInfo fileInfo)
            throws FileSystemException, FileInfoMismatchException, IOException {
        DiskFile diskFile;
        synchronized (diskFiles) {
            final ArchivePath archivePath = fileInfo.getPath();
            for (;;) {
                diskFile = diskFiles.get(archivePath);
                if (diskFile == null) {
                    /*
                     * Create an entry for this file.
                     */
                    do {
                        try {
                            diskFile = new DiskFile(fileInfo);
                        }
                        catch (final FileSystemException e) {
                            // Too many open files
                            if (removeLru() == null) {
                                throw e;
                            }
                        }
                    } while (diskFile == null);
                    diskFiles.put(archivePath, diskFile);
                }
                if (fileInfo.equals(diskFile.getFileInfo())) {
                    break;
                }
                /*
                 * Vet the on-disk file against the given metadata
                 */
                final int cmp = fileInfo.getTime()
                        .compareTo(diskFile.getTime());
                if (cmp < 0) {
                    // A newer version of the file exists
                    return null;
                }
                if (cmp == 0) {
                    // Same-name file but different metadata. Very strange.
                    throw new FileInfoMismatchException(fileInfo,
                            diskFile.getFileInfo());
                }
                // An older version of the file exists. Delete it.
                try {
                    diskFile.delete();
                }
                catch (final NoSuchFileException e) {
                    // The file was deleted by another thread
                    logger.debug(
                            "Older file was deleted by another thread: {}",
                            archivePath);
                }
                finally {
                    diskFiles.remove(archivePath);
                }
            }
            diskFile.lock();
        }
        return diskFile;
    }

    /**
     * Removes the least-recently-used (LRU) disk-file from the map: gets the
     * LRU disk-file, ensures that it's closed, and removes it from the map.
     * 
     * @return The removed disk-file or {@code null} if the map is empty.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private DiskFile removeLru() throws IOException {
        DiskFile diskFile;
        synchronized (diskFiles) {
            final Iterator<Map.Entry<ArchivePath, DiskFile>> iter = diskFiles
                    .entrySet().iterator();
            if (!iter.hasNext()) {
                return null;
            }
            diskFile = iter.next().getValue();
            diskFile.close();
            iter.remove();
        }
        return diskFile;
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
     * @throws FileInfoMismatchException
     *             if the file-information of the given piece doesn't match that
     *             of the extant file except for the {@link FileId}.
     * @throws IOException
     *             if an I/O error occurs.
     */
    boolean exists(final PieceSpec pieceSpec) throws FileSystemException,
            FileInfoMismatchException, IOException {
        final DiskFile diskFile = getDiskFile(pieceSpec.getFileInfo());
        if (diskFile == null) {
            // A newer version of the file exists.
            return true;
        }
        try {
            return diskFile.hasPiece(pieceSpec.getIndex());
        }
        finally {
            diskFile.unlock();
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
     * @throws FileInfoMismatchException
     *             if the file-information of the given piece-specification
     *             doesn't match that of the extant file except for the
     *             {@link FileId}.
     * @throws IOException
     *             if an I/O error occurred.
     */
    Piece getPiece(final PieceSpec pieceSpec) throws FileSystemException,
            FileInfoMismatchException, IOException {
        final DiskFile diskFile = getDiskFile(pieceSpec.getFileInfo());
        if (diskFile == null) {
            // A newer version of the file exists.
            return null;
        }
        try {
            for (;;) {
                try {
                    return diskFile.getPiece(pieceSpec);
                }
                catch (final FileSystemException e) {
                    if (diskFile.equals(removeLru())) {
                        throw e;
                    }
                }
            }
        }
        finally {
            diskFile.unlock();
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
     *             if the file-information of the given piece doesn't match that
     *             of the extant file except for the {@link FileId}.
     * @throws IOException
     *             if an I/O error occurred.
     * @throws NullPointerException
     *             if {@code piece == null}.
     */
    boolean putPiece(final Piece piece) throws FileSystemException,
            NoSuchFileException, FileInfoMismatchException, IOException {
        final FileInfo fileInfo = piece.getFileInfo();
        final DiskFile diskFile = getDiskFile(fileInfo);
        if (diskFile == null) {
            // A newer version of the file exists.
            logger.trace("Newer file version exists: {}", fileInfo);
            return false;
        }
        try {
            for (;;) {
                try {
                    final boolean isComplete = diskFile.putPiece(piece);
                    if (isComplete) {
                        synchronized (dataProductListeners) {
                            for (final DataProductListener listener : dataProductListeners) {
                                final DataProduct product = new DataProduct(
                                        rootDir, fileInfo);
                                listener.process(product);
                            }
                        }
                    }
                    return isComplete;
                }
                catch (final FileSystemException e) {
                    if (diskFile.equals(removeLru())) {
                        throw e;
                    }
                }
            }
        }
        finally {
            diskFile.unlock();
        }
    }

    /**
     * Saves data in the archive.
     * 
     * @param path
     *            The pathname for the data in the archive.
     * @param data
     *            The data.
     * @throws FileAlreadyExistsException
     *             the file is being actively written by another thread.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void save(final ArchivePath path, final ByteBuffer data)
            throws FileAlreadyExistsException, IOException {
        Path hiddenPath = getHiddenAbsolutePath(path);
        Files.createDirectories(hiddenPath.getParent());
        final SeekableByteChannel channel = Files.newByteChannel(hiddenPath,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        try {
            channel.write(data);
            ArchiveTime.adjustTime(hiddenPath);
            final Path visiblePath = getVisiblePath(hiddenPath);
            Files.createDirectories(visiblePath.getParent());
            Files.move(hiddenPath, visiblePath, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            hiddenPath = null;
        }
        finally {
            try {
                channel.close();
            }
            catch (final IOException ignored) {
            }
            if (hiddenPath != null) {
                try {
                    Files.delete(hiddenPath);
                }
                catch (final IOException ignored) {
                }
            }
        }
    }

    /**
     * Saves an object in the archive.
     * 
     * @param archivePath
     *            The pathname for the object in the archive.
     * @param serializable
     *            The object to be saved in the file.
     * @param timeToLive
     *            The time for the file to live in seconds. A value of
     *            {@code -1} means indefinitely.
     * @throws FileAlreadyExistsException
     *             the hidden file was created by another thread.
     * @throws FileSystemException
     *             if too many files are open
     * @throws IOException
     *             if an I/O error occurs.
     */
    private void save(final ArchivePath archivePath,
            final Serializable serializable, final int timeToLive)
            throws FileAlreadyExistsException, FileSystemException, IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(
                byteArrayOutputStream);
        oos.writeObject(serializable);
        final long size = byteArrayOutputStream.size();
        final FileId fileId = new FileId(archivePath);
        final FileInfo fileInfo = new FileInfo(fileId, size,
                FileInfo.getDefaultPieceSize(), timeToLive);
        final DiskFile diskFile;
        try {
            diskFile = getDiskFile(fileInfo);
        }
        catch (final FileInfoMismatchException e) {
            throw (AssertionError) new AssertionError().initCause(e);
        }
        boolean success = false;
        try {
            final ByteBuffer byteBuf = ByteBuffer.wrap(byteArrayOutputStream
                    .toByteArray());
            for (int i = 0; i < fileInfo.getPieceCount(); i++) {
                final byte[] data = new byte[fileInfo.getSize(i)];
                final PieceSpec pieceSpec = new PieceSpec(fileInfo, i);
                byteBuf.get(data);
                diskFile.putPiece(new Piece(pieceSpec, data));
            }
            success = true;
        }
        finally {
            if (!success) {
                diskFile.delete();
            }
            diskFile.unlock();
        }
    }

    /**
     * Saves an object in a hidden file in the archive. The hidden file will not
     * be distributed.
     * 
     * @param archivePath
     *            The pathname for the (visible) file in the archive.
     * @param serializable
     *            The object to be saved in the file.
     * @throws FileAlreadyExistsException
     *             the file is being actively written by another thread.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void hide(final ArchivePath archivePath, final Serializable serializable)
            throws FileAlreadyExistsException, IOException {
        Path hiddenPath = getHiddenAbsolutePath(archivePath);
        Files.createDirectories(hiddenPath.getParent());
        OutputStream outStream = Files.newOutputStream(hiddenPath,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        try {
            ObjectOutputStream objOutStream = new ObjectOutputStream(outStream);
            try {
                objOutStream.writeObject(serializable);
                objOutStream.close();
                objOutStream = null;
                outStream = null;
                ArchiveTime.adjustTime(hiddenPath);
                hiddenPath = null;
            }
            finally {
                if (objOutStream != null) {
                    try {
                        objOutStream.close();
                        outStream = null;
                    }
                    catch (final IOException ignored) {
                    }
                }
            }
        }
        finally {
            if (outStream != null) {
                try {
                    outStream.close();
                }
                catch (final IOException ignored) {
                }
            }
            if (hiddenPath != null) {
                try {
                    Files.delete(hiddenPath);
                }
                catch (final IOException ignored) {
                }
            }
        }
    }

    /**
     * Returns the archive-time of a hidden file in the archive.
     * 
     * @param archivePath
     *            The pathname for the file in the archive.
     * @return The archive-time of the hidden file.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NoSuchFileException
     *             if the hidden file doesn't exist.
     */
    ArchiveTime getArchiveTime(final ArchivePath archivePath)
            throws NoSuchFileException, IOException {
        final Path hiddenPath = getHiddenAbsolutePath(archivePath);
        return new ArchiveTime(hiddenPath);
    }

    /**
     * Reveals a hidden object in the archive.
     * 
     * @param archivePath
     *            The pathname for the object in the archive.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void reveal(final ArchivePath archivePath) throws IOException {
        final Path hiddenPath = getHiddenAbsolutePath(archivePath);
        final Path visiblePath = getVisiblePath(hiddenPath);
        Files.createDirectories(visiblePath.getParent());
        Files.move(hiddenPath, visiblePath, StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Ensures that a hidden file doesn't exist.
     * 
     * @param archivePath
     *            Pathname of the file in the archive.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void removeHidden(final ArchivePath archivePath) throws IOException {
        final Path hiddenPath = getHiddenAbsolutePath(archivePath);
        Files.deleteIfExists(hiddenPath);
    }

    /**
     * Restores an object from a file.
     * 
     * @param archivePath
     *            Pathname of the file in the archive.
     * @param type
     *            Expected type of the restored object.
     * @throws ClassNotFoundException
     *             if the type of the restored object is unknown.
     * @throws ClassCastException
     *             if the object isn't the expected type.
     * @throws FileNotFoundException
     *             if the file doesn't exist.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws StreamCorruptedException
     *             if the file is corrupt.
     */
    Object restore(final ArchivePath archivePath, final Class<?> type)
            throws FileNotFoundException, IOException, ClassNotFoundException {
        final Path path = resolve(archivePath);
        InputStream inputStream = Files.newInputStream(path);
        try {
            final ObjectInputStream ois = new ObjectInputStream(inputStream);
            try {
                final Object obj = ois.readObject();
                if (!type.isInstance(obj)) {
                    throw new ClassCastException("expected=" + type
                            + ", actual=" + obj.getClass());
                }
                return obj;
            }
            finally {
                try {
                    ois.close();
                    inputStream = null;
                }
                catch (final IOException ignored) {
                }
            }
        }
        catch (final StreamCorruptedException e) {
            throw (StreamCorruptedException) new StreamCorruptedException(
                    "Corrupted file: " + path).initCause(e);
        }
        finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                }
                catch (final IOException ignored) {
                }
            }
        }
    }

    /**
     * Removes a file from both the visible and hidden directory hierarchies.
     * 
     * @param archivePath
     *            Pathname of the file to be removed.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void remove(final ArchivePath archivePath) throws IOException {
        synchronized (diskFiles) {
            final DiskFile diskFile = diskFiles.get(archivePath);
            if (diskFile != null) {
                diskFile.delete();
                diskFiles.remove(archivePath);
            }
            else {
                // The following works because renaming a file is atomic
                final Path visiblePath = archivePath.getAbsolutePath(rootDir);
                if (!remove(visiblePath)) {
                    final Path hiddenPath = pathname.hide(visiblePath);
                    if (!remove(hiddenPath)) {
                        remove(visiblePath);
                    }
                }
            }
        }
    }

    /**
     * Removes the file that matches a file identifier from both the visible and
     * hidden directory hierarchies.
     * 
     * @param fileId
     *            Identifier of the file to be removed. All attributes must be
     *            matched.
     * @throws IOException
     *             if an I/O error occurs.
     */
    void remove(final FileId fileId) throws IOException {
        remove(fileId.getPath());
    }

    /**
     * Removes a file or directory corresponding to a pathname if it exists. The
     * file or directory can be a hidden one.
     * 
     * @param path
     *            The pathname of the file or directory.
     * @return {@code true} if and only if the file or directory existed and was
     *         deleted.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private boolean remove(final Path path) throws IOException {
        if (!Files.exists(path)) {
            logger.trace("File doesn't exist: {}", path);
            return false;
        }
        logger.trace("Removing file {}", path);
        final EnumSet<FileVisitOption> opts = EnumSet
                .of(FileVisitOption.FOLLOW_LINKS);
        Files.walkFileTree(path, opts, Integer.MAX_VALUE,
                new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult postVisitDirectory(final Path dir,
                            final IOException e) throws IOException {
                        if (null != e) {
                            throw e;
                        }
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(final Path path,
                            final BasicFileAttributes attr) throws IOException {
                        if (!attr.isDirectory()) {
                            Files.delete(path);
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
        return true;
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
        return pathname.hide(path);
    }

    /**
     * Returns the visible form of a hidden pathname
     * 
     * @param path
     *            The hidden pathname whose visible form is to be returned.
     * @return The visible form of {@code path}.
     */
    Path getVisiblePath(final Path path) {
        return pathname.reveal(path);
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
     * Returns the absolute pathname of the hidden form of a visible pathname
     * that's relative to the archive.
     * 
     * @param path
     *            The visible pathname relative to the archive.
     * @return The corresponding absolute, hidden pathname.
     */
    private Path getHiddenAbsolutePath(final ArchivePath archivePath) {
        return getHiddenAbsolutePath(archivePath.getPath());
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
                        return pathname.isHidden(dir)
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
            synchronized (diskFiles) {
                for (final Iterator<DiskFile> iter = diskFiles.values()
                        .iterator(); iter.hasNext();) {
                    iter.next().close();
                    iter.remove();
                }
            }
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
