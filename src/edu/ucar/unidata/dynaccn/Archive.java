/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.FileNotFoundException;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystemException;
import java.nio.file.FileSystems;
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
import java.nio.file.StandardWatchEventKind;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.Attributes;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.NotThreadSafe;
import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An archive of files.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class Archive {
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
         * The associated server.
         */
        private final Server              server;

        /**
         * Constructs from the server to notify about new files. Doesn't return.
         * 
         * @param server
         *            The server to notify about new files.
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
                watchService = FileSystems.getDefault().newWatchService();
                try {
                    registerAll(rootDir);
                    for (;;) {
                        final WatchKey key = watchService.take();
                        for (final WatchEvent<?> event : key.pollEvents()) {
                            final WatchEvent.Kind<?> kind = event.kind();
                            if (kind == StandardWatchEventKind.OVERFLOW) {
                                logger
                                        .error(
                                                "Couldn't keep-up watching file-tree rooted at \"{}\"",
                                                rootDir);
                            }
                            else {
                                final Path name = (Path) event.context();
                                Path path = dirs.get(key);
                                path = path.resolve(name);
                                if (kind == StandardWatchEventKind.ENTRY_CREATE) {
                                    newFile(path);
                                }
                                else if (kind == StandardWatchEventKind.ENTRY_DELETE) {
                                    removedFile(path);
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
         * @throws IOException
         *             if an I/O error occurs.
         */
        private void newFile(final Path path) throws IOException {
            final BasicFileAttributes attributes = Attributes
                    .readBasicFileAttributes(path);
            if (attributes.isDirectory()) {
                registerAll(path);
                walkDirectory(path, new FilePieceSpecSetConsumer() {
                    @Override
                    public void consume(final FilePieceSpecSet spec) {
                        server.newData(spec);
                    }
                }, Predicate.EVERYTHING);
            }
            else if (attributes.isRegularFile()) {
                final FileInfo fileInfo = new FileInfo(new FileId(rootDir
                        .relativize(path)), attributes.size(), PIECE_SIZE);
                server.newData(FilePieceSpecSet.newInstance(fileInfo, true));
            }
        }

        /**
         * Handles the removal of a file.
         * 
         * @param path
         *            The pathname of the removed file.
         * @throws IOException
         *             if an I/O error occurs.
         */
        private void removedFile(final Path path) throws IOException {
            final WatchKey k = keys.remove(path);
            if (null != k) {
                dirs.remove(k);
                k.cancel();
            }
            // Obviated by addition of file-deleter to this class.
            // final Path relativePath = rootDir.relativize(path);
            // final FileId fileId = new FileId(relativePath);
            // server.removed(fileId);
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
                    StandardWatchEventKind.ENTRY_CREATE,
                    StandardWatchEventKind.ENTRY_DELETE);
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
                    .of(FileVisitOption.FOLLOW_LINKS,
                            FileVisitOption.DETECT_CYCLES);
            Files.walkFileTree(dir, opts, Integer.MAX_VALUE,
                    new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult preVisitDirectory(final Path dir) {
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
     * Util class for hiding and revealing files based on pathnames.
     * 
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
        private SeekableByteChannel channel;
        /**
         * Whether or not the file is or should be writable.
         */
        @GuardedBy("lock")
        private boolean             writable;
        /**
         * The lock for this instance.
         */
        private final ReentrantLock lock = new ReentrantLock();

        /**
         * Constructs from a pathname and the number of pieces in the file. If
         * the file exists, then it is opened read-only; otherwise, it is opened
         * as a writable, but hidden, file.
         * 
         * @param path
         *            The pathname of the file.
         * @param pieceCount
         *            The number of pieces in the file.
         * @throws FileNotFoundException
         *             if the file doesn't exist and can't be created.
         * @throws FileSystemException
         *             if too many files are open.
         * @throws NullPointerException
         *             if {@code path == null}.
         */
        DiskFile(final Path path, final int pieceCount)
                throws FileSystemException, IOException {
            lock.lock();
            try {
                if (path.exists()) {
                    this.path = path;
                    writable = false;
                    open();
                    indexes = new CompleteBitSet(pieceCount);
                }
                else {
                    this.path = pathname.hide(path);
                    Files.createDirectories(path.getParent());
                    writable = true;
                    open();
                    indexes = FiniteBitSet.newInstance(pieceCount);
                }
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
         * Opens the file.
         * 
         * @throws FileSystemException
         *             if too many files are open.
         * @throws IOException
         *             if an I/O error occurs.
         */
        private void open() throws FileSystemException, IOException {
            lock.lock();
            try {
                if (channel == null) {
                    if (!writable) {
                        channel = path.newByteChannel(StandardOpenOption.READ);
                    }
                    else {
                        Files.createDirectories(path.getParent());
                        channel = path.newByteChannel(StandardOpenOption.READ,
                                StandardOpenOption.WRITE,
                                StandardOpenOption.CREATE);
                    }
                }
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
                final boolean isComplete;
                if (indexes.isSet(index)) {
                    isComplete = false;
                }
                else {
                    open();
                    channel.position(piece.getOffset());
                    final ByteBuffer buf = ByteBuffer.wrap(piece.getData());
                    while (buf.hasRemaining()) {
                        channel.write(buf);
                    }
                    indexes = indexes.setBit(index);
                    isComplete = indexes.areAllSet();
                    if (isComplete) {
                        final Path newPath = pathname.reveal(path);
                        for (;;) {
                            try {
                                Files.createDirectories(newPath.getParent());
                                break;
                            }
                            catch (final NoSuchFileException e) {
                                // A directory in the path was just deleted
                            }
                        }
                        close();
                        for (;;) {
                            try {
                                path.moveTo(newPath,
                                        StandardCopyOption.ATOMIC_MOVE);
                                final int timeToLive = piece.getTimeToLive();
                                if (timeToLive >= 0) {
                                    /*
                                     * TODO: make the combination of future
                                     * file-deletion and file moving more robust
                                     * in the face of power failures and very
                                     * short lifetimes.
                                     */
                                    fileDeleter.delete(newPath,
                                            1000 * timeToLive);
                                }
                                break;
                            }
                            catch (final NoSuchFileException e) {
                                // A directory in the path was just deleted
                            }
                        }
                        path = newPath;
                        writable = false;
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
                final ByteBuffer buf = ByteBuffer.wrap(data);
                open();
                channel.position(pieceSpec.getOffset());
                int nread;
                do {
                    nread = channel.read(buf);
                } while (nread != -1 && buf.hasRemaining());
                return new Piece(pieceSpec, data);
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Closes this instance. Does nothing if the file is already closed.
         * 
         * @throws IOException
         *             if an I/O error occurs.
         */
        void close() throws IOException {
            lock.lock();
            try {
                if (channel != null) {
                    channel.close();
                    channel = null;
                }
            }
            finally {
                lock.unlock();
            }
        }
    }

    private final class DiskFileMap extends LinkedHashMap<Path, DiskFile> {
        /**
         * The serial version identifier .
         */
        private static final long serialVersionUID = 1L;

        private DiskFileMap() {
            super(16, 0.75f, true);
        }

        @Override
        protected boolean removeEldestEntry(
                final Map.Entry<Path, DiskFile> entry) {
            if (size() > MAX_OPEN_FILES) {
                final DiskFile diskFile = entry.getValue();
                diskFile.lock();
                try {
                    diskFile.close();
                    diskFiles.remove(entry.getKey());
                }
                catch (final IOException e) {
                    logger.error("Couldn't close file " + diskFile, e);
                }
                finally {
                    diskFile.unlock();
                }
            }
            return false;
        }
    }

    /**
     * The logger for this class.
     */
    private static final Logger logger         = LoggerFactory
                                                       .getLogger(Archive.class);
    /**
     * The name of the hidden directory that will be ignored for the most part.
     */
    private static final Path   HIDDEN_DIR     = Paths.get(".dynaccn");
    /**
     * The canonical size, in bytes, of a piece of data (131072).
     */
    private static final int    PIECE_SIZE     = 0x20000;
    /**
     * The maximum number of open files.
     */
    private static final int    MAX_OPEN_FILES = 128;
    /**
     * The pathname utility for hidden pathnames.
     */
    private final Pathname      pathname       = new Pathname();
    /**
     * The set of active disk files.
     */
    @GuardedBy("itself")
    private final DiskFileMap   diskFiles      = new DiskFileMap();
    /**
     * The pathname of the root of the file-tree.
     */
    private final Path          rootDir;
    /**
     * The file-deleter.
     */
    private final FileDeleter   fileDeleter;

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
     * Constructs from the pathname of the root of the file-tree.
     * 
     * @param rootDir
     *            The pathname of the root of the file-tree.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code rootDir == null}.
     */
    Archive(final Path rootDir) throws IOException {
        if (null == rootDir) {
            throw new NullPointerException();
        }
        final Path hiddenDir = rootDir.resolve(HIDDEN_DIR);
        Files.createDirectories(hiddenDir);
        /*
         * According to the Java 7 tutorial, the following is valid:
         * 
         * Attributes.setAttribute(hiddenDir, "dos:hidden", true);
         * 
         * but the given method doesn't exist in reality. Hence, the following:
         */
        try {
            final Boolean hidden = (Boolean) hiddenDir.getAttribute(
                    "dos:hidden", LinkOption.NOFOLLOW_LINKS);
            if (null != hidden && !hidden) {
                // The file-system is DOS and the hidden directory isn't hidden
                hiddenDir.setAttribute("dos:hidden", Boolean.TRUE,
                        LinkOption.NOFOLLOW_LINKS);
            }
        }
        catch (final FileSystemException ignored) {
            // The file-system isn't DOS
        }
        this.rootDir = rootDir;
        final Path fileDeletionQueuePath = hiddenDir
                .resolve("fileDeletionQueue");
        this.fileDeleter = new FileDeleter(rootDir, new FileDeletionQueue(
                fileDeletionQueuePath));
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
     * Returns the disk-file associated with a file. Creates the disk-file if it
     * doesn't already exist. The disk-file is returned in a locked state.
     * 
     * @param fileInfo
     *            Information on the file
     * @return The associated, locked disk-file
     * @throws FileSystemException
     *             if too many files are open. The map is now empty and all
     *             files are closed.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code fileInfo == null}.
     */
    private DiskFile getDiskFile(final FileInfo fileInfo)
            throws FileSystemException, IOException {
        DiskFile diskFile;
        final Path path = fileInfo.getPath();
        synchronized (diskFiles) {
            diskFile = diskFiles.get(path);
            if (null == diskFile) {
                /*
                 * This file can't be open because it doesn't have an entry.
                 */
                for (;;) {
                    try {
                        diskFile = new DiskFile(rootDir.resolve(path), fileInfo
                                .getPieceCount());
                        break;
                    }
                    catch (final FileSystemException e) {
                        // Too many open files
                        if (removeLru() == null) {
                            throw e;
                        }
                    }
                }
                diskFiles.put(path, diskFile);
            }
            diskFile.lock();
        }
        return diskFile;
    }

    /**
     * Removes the least-recently-used (LRU) disk-file from the map: gets the
     * LRU disk-file, removes it from the map, and closes it.
     * 
     * @return The removed disk-file or {@code null} if the map is empty.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private DiskFile removeLru() throws IOException {
        DiskFile diskFile;
        synchronized (diskFiles) {
            final Iterator<DiskFile> iter = diskFiles.values().iterator();
            if (!iter.hasNext()) {
                return null;
            }
            diskFile = iter.next();
            diskFile.lock();
            try {
                iter.remove();
                diskFile.close();
            }
            finally {
                diskFile.unlock();
            }
        }
        return diskFile;
    }

    /**
     * Writes a piece of data.
     * 
     * @param piece
     *            Piece of data to be written.
     * @return {@code true} if and only if the file is now complete.
     * @throws FileNotFoundException
     *             if the destination file doesn't exist.
     * @throws FileSystemException
     *             if too many files are open.
     * @throws IOException
     *             if an I/O error occurred.
     * @throws NullPointerException
     *             if {@code piece == null}.
     */
    boolean putPiece(final Piece piece) throws FileSystemException, IOException {
        final DiskFile diskFile = getDiskFile(piece.getFileInfo());
        try {
            for (;;) {
                try {
                    return diskFile.putPiece(piece);
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
     * Returns a piece of data.
     * 
     * @param pieceSpec
     *            Information on the piece of data.
     * @return The piece of data.
     * @throws FileSystemException
     *             if too many files are open.
     * @throws IOException
     *             if an I/O error occurred.
     */
    Piece getPiece(final PieceSpec pieceSpec) throws FileSystemException,
            IOException {
        final DiskFile diskFile = getDiskFile(pieceSpec.getFileInfo());
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
        final DiskFile diskFile = getDiskFile(pieceSpec.getFileInfo());
        try {
            return diskFile.hasPiece(pieceSpec.getIndex());
        }
        finally {
            diskFile.unlock();
        }
    }

    /**
     * Removes a file or category.
     * 
     * @param fileId
     *            Specification of the file or category to be removed.
     * @throws IOError
     *             if an I/O error occurs. The error will have as its cause the
     *             underlying IOException that was thrown.
     */
    void remove(final FileId fileId) {
        Path path = rootDir.resolve(fileId.getPath());
        remove(path);
        path = pathname.hide(path);
        remove(path);
    }

    /**
     * Removes a file or directory corresponding to a pathname if it exists. The
     * file or directory can be a hidden one.
     * 
     * @param path
     *            The pathname of the file or directory.
     * @throws IOError
     *             if an I/O error occurs. The error will have as its cause the
     *             underlying IOException that was thrown.
     */
    private void remove(final Path path) {
        if (path.exists()) {
            final EnumSet<FileVisitOption> opts = EnumSet
                    .of(FileVisitOption.FOLLOW_LINKS,
                            FileVisitOption.DETECT_CYCLES);
            Files.walkFileTree(path, opts, Integer.MAX_VALUE,
                    new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult postVisitDirectory(
                                final Path dir, final IOException e) {
                            if (null != e) {
                                throw new IOError(e);
                            }
                            try {
                                dir.delete();
                            }
                            catch (final IOException e1) {
                                throw new IOError(e1);
                            }
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFile(final Path path,
                                final BasicFileAttributes attr) {
                            if (!attr.isDirectory()) {
                                try {
                                    path.delete();
                                }
                                catch (final IOException e) {
                                    throw new IOError(e);
                                }
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    });
        }
    }

    /**
     * Returns the hidden form of a pathname.
     * 
     * @param path
     *            The pathname whose hidden form is to be returned.
     * @return The hidden form of {@code path}.
     */
    Path getHiddenForm(final Path path) {
        return pathname.hide(path);
    }

    /**
     * Returns the visible form of a hidden pathname
     * 
     * @param path
     *            The hidden pathname whose visible form is to be returned.
     * @return The visible form of {@code path}.
     */
    Path getVisibleForm(final Path path) {
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
        return rootDir.resolve(getHiddenForm(path));
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
     * @param predicate
     *            The selection criteria.
     */
    private void walkDirectory(final Path root,
            final FilePieceSpecSetConsumer consumer, final Predicate predicate) {
        final EnumSet<FileVisitOption> opts = EnumSet.of(
                FileVisitOption.FOLLOW_LINKS, FileVisitOption.DETECT_CYCLES);
        Files.walkFileTree(root, opts, Integer.MAX_VALUE,
                new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult preVisitDirectory(final Path dir) {
                        return pathname.isHidden(dir)
                                ? FileVisitResult.SKIP_SUBTREE
                                : FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(final Path path,
                            final BasicFileAttributes attr) {
                        if (attr.isRegularFile()) {
                            final FileId fileId = new FileId(rootDir
                                    .relativize(path));
                            final FileInfo fileInfo = new FileInfo(fileId, attr
                                    .size(), PIECE_SIZE);
                            if (predicate.satisfiedBy(fileInfo)) {
                                consumer.consume(FilePieceSpecSet.newInstance(
                                        fileInfo, true));
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
     * @param predicate
     *            The selection criteria.
     */
    void walkArchive(final FilePieceSpecSetConsumer consumer,
            final Predicate predicate) {
        walkDirectory(rootDir, consumer, predicate);
    }

    /**
     * Watches the archive for new files and removed files and directories.
     * Ignores hidden directories. Doesn't return.
     * 
     * @param server
     *            The server.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     */
    void watchArchive(final Server server) throws IOException,
            InterruptedException {
        new FileWatcher(server);
    }

    /**
     * Closes this instance. Closes all open files and the file-deleter.
     * 
     * @throws IOException
     *             if an I/O error occurs.
     */
    void close() throws IOException {
        try {
            fileDeleter.close();
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
