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
import java.nio.file.FileSystems;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
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
            if (diskFiles.size() > MAX_OPEN_FILES) {
                final DiskFile diskFile = entry.getValue();
                try {
                    entry.getValue().close();
                }
                catch (final IOException e) {
                    logger.error("Couldn't close file " + diskFile, e);
                }
                return true;
            }
            return false;
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
            final Path relativePath = rootDir.relativize(path);
            final FileId fileId = new FileId(relativePath);
            final WatchKey k = keys.remove(path);
            if (null != k) {
                dirs.remove(k);
                k.cancel();
            }
            server.removed(fileId);
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
        @GuardedBy("this")
        private FiniteBitSet        indexes;
        /**
         * The pathname of the file.
         */
        @GuardedBy("this")
        private Path                path;
        /**
         * The I/O channel for the file.
         */
        @GuardedBy("this")
        private SeekableByteChannel channel;

        /**
         * Constructs from a pathname and the number of pieces in the file. If
         * the file exists, then it is opened read-only; otherwise, it is opened
         * as a writable, hidden file.
         * 
         * @param path
         *            The pathname of the file.
         * @param pieceCount
         *            The number of pieces in the file.
         * @throws FileNotFoundException
         *             if the file doesn't exist and can't be created.
         * @throws NullPointerException
         *             if {@code path == null}.
         */
        DiskFile(Path path, final int pieceCount) throws IOException {
            synchronized (this) {
                if (path.exists()) {
                    channel = path.newByteChannel(StandardOpenOption.READ);
                    indexes = new CompleteBitSet(pieceCount);
                }
                else {
                    path = pathname.hide(path);
                    Files.createDirectories(path.getParent());
                    // DSYNC isn't used because the file is closed when complete
                    channel = path
                            .newByteChannel(StandardOpenOption.READ,
                                    StandardOpenOption.WRITE,
                                    StandardOpenOption.CREATE);
                    indexes = FiniteBitSet.newInstance(pieceCount);
                }
                this.path = path;
            }
        }

        /**
         * Writes a piece of data.
         * 
         * @param piece
         *            The piece of data.
         * @return {@code true} if and only if the file is complete, in which
         *         case the file is closed.
         * @throws FileNotFoundException
         *             if the file has been deleted.
         * @throws IOException
         *             if an I/O error occurs.
         * @throws NullPointerException
         *             if {@code piece == null}.
         */
        synchronized boolean putPiece(final Piece piece) throws IOException {
            if (!path.exists()) {
                throw new FileNotFoundException();
            }
            else {
                final int index = piece.getIndex();
                final boolean isComplete;

                if (indexes.isSet(index)) {
                    isComplete = false;
                }
                else {
                    channel.position(piece.getOffset());
                    final ByteBuffer buf = ByteBuffer.wrap(piece.getData());
                    while (buf.hasRemaining()) {
                        channel.write(buf);
                    }

                    indexes = indexes.setBit(index);
                    isComplete = indexes.areAllSet();

                    if (isComplete) {
                        channel.close();
                        final Path newPath = pathname.reveal(path);
                        Files.createDirectories(newPath.getParent());
                        path.moveTo(newPath);
                        channel = newPath
                                .newByteChannel(StandardOpenOption.READ);
                        path = newPath;
                    }
                }

                return isComplete;
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
        synchronized boolean hasPiece(final int index) {
            return indexes.isSet(index);
        }

        /**
         * Returns a piece of data.
         * 
         * @param pieceSpec
         *            Information on the piece of data.
         * @throws FileNotFoundException
         *             if the file has been deleted.
         * @throws IOException
         *             if an I/O error occurs.
         */
        synchronized Piece getPiece(final PieceSpec pieceSpec)
                throws IOException {
            if (!path.exists()) {
                throw new FileNotFoundException();
            }
            else {
                final byte[] data = new byte[pieceSpec.getSize()];
                final ByteBuffer buf = ByteBuffer.wrap(data);

                channel.position(pieceSpec.getOffset());
                int nread;
                do {
                    nread = channel.read(buf);
                } while (nread != -1 && buf.hasRemaining());

                return new Piece(pieceSpec, data);
            }
        }

        /**
         * Closes this instance.
         * 
         * @throws IOException
         *             if an I/O error occurs.
         */
        synchronized void close() throws IOException {
            channel.close();
        }
    }

    /**
     * Utility class for hiding and revealing files based on pathnames.
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
    private static final int    MAX_OPEN_FILES = 512;
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
        final Boolean hidden = (Boolean) hiddenDir.getAttribute("dos:hidden",
                LinkOption.NOFOLLOW_LINKS);
        if (null != hidden && !hidden) {
            // The file-system is DOS and the hidden directory isn't hidden
            hiddenDir.setAttribute("dos:hidden", Boolean.TRUE,
                    LinkOption.NOFOLLOW_LINKS);
        }
        this.rootDir = rootDir;
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
     * Returns the pathname of the root directory of the file-tree.
     * 
     * @return Pathname of the root directory of the file-tree.
     */
    Path getRootDir() {
        return rootDir;
    }

    /**
     * Returns the disk-file associated with a file. Creates the instance if it
     * doesn't already exist.
     * 
     * @param fileInfo
     *            Information on the file
     * @return The associated instance.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code fileInfo == null}.
     */
    private DiskFile getDiskFile(final FileInfo fileInfo) throws IOException {
        DiskFile diskFile;
        final Path path = fileInfo.getPath();
        synchronized (diskFiles) {
            diskFile = diskFiles.get(path);
            if (null == diskFile) {
                /*
                 * This file can't be open because it doesn't have an entry.
                 */
                diskFile = new DiskFile(rootDir.resolve(path), fileInfo
                        .getPieceCount());
                diskFiles.put(path, diskFile);
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
     * @throws IOException
     *             if an I/O error occurred.
     * @throws NullPointerException
     *             if {@code piece == null}.
     */
    boolean putPiece(final Piece piece) throws IOException {
        final DiskFile diskFile = getDiskFile(piece.getFileInfo());
        return diskFile.putPiece(piece);
    }

    /**
     * Returns a piece of data.
     * 
     * @param pieceSpec
     *            Information on the piece of data.
     * @return The piece of data.
     * @throws IOException
     *             if an I/O error occurred.
     */
    Piece getPiece(final PieceSpec pieceSpec) throws IOException {
        return getDiskFile(pieceSpec.getFileInfo()).getPiece(pieceSpec);
    }

    /**
     * Indicates whether or not a piece of data exists.
     * 
     * @param dir
     *            Pathname of the output directory.
     * @param pieceSpec
     *            Specification of the piece of data.
     * @return {@code true} if and only if the piece of data exists.
     * @throws IOException
     *             if an I/O error occurs.
     */
    boolean exists(final PieceSpec pieceSpec) throws IOException {
        return getDiskFile(pieceSpec.getFileInfo()).hasPiece(
                pieceSpec.getIndex());
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
                                        new FileInfo(fileId, attr.size(),
                                                PIECE_SIZE), true));
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
    Path hide(final Path path) {
        return pathname.hide(path);
    }

    /**
     * Returns the visible form of a hidden pathname
     * 
     * @param path
     *            The hidden pathname whose visible form is to be returned.
     * @return The visible form of {@code path}.
     */
    Path reveal(final Path path) {
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
        return rootDir.resolve(hide(path));
    }

    /**
     * Closes this instance. Closes all open files.
     * 
     * @throws IOException
     *             if an I/O error occurs.
     */
    void close() throws IOException {
        synchronized (diskFiles) {
            for (final Iterator<DiskFile> iter = diskFiles.values().iterator(); iter
                    .hasNext();) {
                iter.next().close();
                iter.remove();
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
