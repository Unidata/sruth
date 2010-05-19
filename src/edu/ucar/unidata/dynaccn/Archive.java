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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.NotThreadSafe;
import net.jcip.annotations.ThreadSafe;

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
        private final Map<WatchKey, Path> dirPaths = new HashMap<WatchKey, Path>();

        /**
         * Constructs from a consumer of file-based data-specifications. Doesn't
         * return.
         * 
         * @throws IOException
         *             if an I/O error occurs.
         * @throws InterruptedException
         *             if the current thread is interrupted.
         */
        FileWatcher(final FileSpecConsumer consumer) throws IOException,
                InterruptedException {
            watchService = FileSystems.getDefault().newWatchService();
            try {
                registerAll(rootDir);
                for (;;) {
                    final WatchKey key = watchService.take();
                    for (final WatchEvent<?> event : key.pollEvents()) {
                        final WatchEvent.Kind<?> kind = event.kind();
                        if (kind == StandardWatchEventKind.OVERFLOW) {
                            System.err
                                    .println("Couldn't keep-up watching file-tree rooted at \""
                                            + rootDir + "\"");
                        }
                        else if (kind == StandardWatchEventKind.ENTRY_CREATE) {
                            final Path name = (Path) event.context();
                            final Path path = dirPaths.get(key).resolve(name);
                            if (!Pathname.isHidden(path)) {
                                final BasicFileAttributes attributes = Attributes
                                        .readBasicFileAttributes(path);
                                if (attributes.isDirectory()) {
                                    registerAll(path);
                                }
                                else if (attributes.isRegularFile()) {
                                    final FileInfo fileInfo = new FileInfo(
                                            new FileId(rootDir.relativize(path)),
                                            attributes.size(), PIECE_SIZE);
                                    consumer.consume(PiecesSpec.newInstance(
                                            fileInfo, true));
                                }
                            }
                        }
                    }
                    if (!key.reset()) {
                        dirPaths.remove(key);
                    }
                }
            }
            finally {
                watchService.close();
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
                    StandardWatchEventKind.ENTRY_CREATE);
            dirPaths.put(key, dir);
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
                            if (Pathname.isHidden(dir)) {
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
         * Constructs from a pathname and the number of pieces in the file.
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
                    path = Pathname.hide(path);
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
         * @return {@code true} if and only if the file is complete.
         * @throws IOException
         *             if an I/O error occurs.
         * @throws NullPointerException
         *             if {@code piece == null}.
         */
        synchronized boolean putPiece(final Piece piece) throws IOException {
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
                    final Path newPath = Pathname.reveal(path);
                    path.moveTo(newPath);
                    path = newPath;
                    channel = path.newByteChannel(StandardOpenOption.READ);
                }
            }

            return isComplete;
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
         * @throws IOException
         *             if an I/O error occurs.
         */
        synchronized Piece getPiece(final PieceSpec pieceSpec)
                throws IOException {
            final byte[] data = new byte[pieceSpec.getSize()];
            final ByteBuffer buf = ByteBuffer.wrap(data);

            channel.position(pieceSpec.getOffset());
            int nread;
            do {
                nread = channel.read(buf);
            } while (nread != -1 && buf.hasRemaining());

            return new Piece(pieceSpec, data);
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
     * The canonical size, in bytes, of a piece of data (131072).
     */
    private static final int                    PIECE_SIZE = 0x20000;
    /**
     * The set of active disk files.
     */
    private final ConcurrentMap<Path, DiskFile> diskFiles  = new ConcurrentHashMap<Path, DiskFile>();
    /**
     * The pathname of the root of the file-tree.
     */
    private final Path                          rootDir;

    /**
     * Constructs from the pathname of the root of the file-tree.
     * 
     * @param rootDir
     *            The pathname of the root of the file-tree.
     * @throws NullPointerException
     *             if {@code rootDir == null}.
     */
    Archive(final Path rootDir) {
        this.rootDir = rootDir;
    }

    /**
     * Constructs from the pathname of the root of the file-tree.
     * 
     * @param rootDir
     *            The pathname of the root of the file-tree.
     * @throws NullPointerException
     *             if {@code rootDir == null}.
     */
    Archive(final String string) {
        this(Paths.get(string));
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
        final Path path = fileInfo.getPath();
        DiskFile diskFile = diskFiles.get(path);

        if (null == diskFile) {
            diskFile = new DiskFile(rootDir.resolve(path), fileInfo
                    .getPieceCount());
            final DiskFile prevDiskFile = diskFiles.putIfAbsent(path, diskFile);

            if (null != prevDiskFile) {
                diskFile.close();
                diskFile = prevDiskFile;
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
        return getDiskFile(piece.getFileInfo()).putPiece(piece);
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
     * Visits all the file-based data-specifications in the archive that match a
     * selection criteria. Doesn't visit files in hidden directories.
     * 
     * @param consumer
     *            The consumer of file-based data-specifications.
     * @param predicate
     *            The selection criteria.
     * @return
     */
    void walkArchive(final FileSpecConsumer consumer, final Predicate predicate) {
        final EnumSet<FileVisitOption> opts = EnumSet.of(
                FileVisitOption.FOLLOW_LINKS, FileVisitOption.DETECT_CYCLES);
        Files.walkFileTree(rootDir, opts, Integer.MAX_VALUE,
                new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult preVisitDirectory(final Path dir) {
                        return Pathname.isHidden(dir)
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
                                consumer.consume(PiecesSpec.newInstance(
                                        new FileInfo(fileId, attr.size(),
                                                PIECE_SIZE), true));
                            }
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
    }

    /**
     * Watches the archive for new files. Doesn't watch for files in hidden
     * directories. Doesn't return.
     * 
     * @param consumer
     *            The consumer of file-based data-specifications.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     * @throws IOException
     */
    void watchArchive(final FileSpecConsumer consumer) throws IOException,
            InterruptedException {
        new FileWatcher(consumer);
    }
}
