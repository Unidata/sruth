/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.BitSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A data-file that resides on a disk.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class DiskFile {
    /**
     * The set of instances.
     */
    private static final ConcurrentMap<FileInfo, DiskFile> diskFiles = new ConcurrentHashMap<FileInfo, DiskFile>();
    /**
     * The random-access-file.
     */
    private final RandomAccessFile                         raf;
    /**
     * The set of existing pieces.
     */
    private final BitSet                                   indexes;
    /**
     * The number of remaining data-pieces to be written.
     */
    private int                                            pieceCount;

    /**
     * Constructs from file-information.
     * 
     * @param dir
     *            The pathname of the directory.
     * @param fileInfo
     *            Information on the file.
     * @throws FileNotFoundException
     *             if the file doesn't exist and can't be created.
     * @throws IllegalArgumentException
     *             if {@code !dir.isAbsolute()}.
     * @throws NullPointerException
     *             if {@code dir == null || fileInfo == null}.
     */
    private DiskFile(final File dir, final FileInfo fileInfo)
            throws FileNotFoundException {
        final File path = fileInfo.getFile(dir);
        final File parent = path.getParentFile();

        parent.mkdirs();

        raf = new RandomAccessFile(path, "rwd");
        pieceCount = fileInfo.getPieceCount();
        indexes = new BitSet(pieceCount > 0
                ? pieceCount - 1
                : 0);
    }

    /**
     * Returns the instance associated with a file.
     * 
     * @param dir
     *            Pathname of the directory.
     * @param fileInfo
     *            Information on the file
     * @return The associated instance.
     * @param forWriting
     *            Whether or not the file should be opened for writing.
     * @throws IllegalArgumentException
     *             if {@code !dir.isAbsolute()}.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code dir == null || fileInfo == null}.
     */
    private static DiskFile getInstance(final File dir, final FileInfo fileInfo)
            throws IOException {
        DiskFile diskFile = diskFiles.get(fileInfo);

        if (null == diskFile) {
            diskFile = new DiskFile(dir, fileInfo);
            final DiskFile prevDiskFile = diskFiles.putIfAbsent(fileInfo,
                    diskFile);

            if (null != prevDiskFile) {
                diskFile.close();
                diskFile = prevDiskFile;
            }
        }

        return diskFile;
    }

    /**
     * Ensures that an on-disk file exists.
     * 
     * @param dir
     *            Pathname of the output directory.
     * @param fileInfo
     *            Information on the file.
     * @throws IllegalArgumentException
     *             if {@code !dir.isAbsolute()}.
     * @throws IOException
     *             in an I/O error occurs.
     * @throws NullPointerException
     *             if {@code dir == null || fileInfo == null}.
     */
    public static void create(final File dir, final FileInfo fileInfo)
            throws IOException {
        getInstance(dir, fileInfo);
    }

    /**
     * Writes a piece of data.
     * 
     * @param dir
     *            Pathname of the output directory.
     * @param piece
     *            Piece of data to be written.
     * @return {@code true} if and only if the file is now complete.
     * @throws IOException
     *             if an I/O error occurred.
     * @throws NullPointerException
     *             if {@code piece == null}.
     */
    static boolean putPiece(final File dir, final Piece piece)
            throws IOException {
        final DiskFile diskFile = getInstance(dir, piece.getFileInfo());

        return diskFile.putPiece(piece);
    }

    /**
     * Returns a piece of data.
     * 
     * @param dir
     *            Pathname of the input directory.
     * @param pieceInfo
     *            Information on the piece of data.
     * @return The piece of data.
     * @throws IOException
     *             if an I/O error occurred.
     */
    static Piece getPiece(final File dir, final PieceInfo pieceInfo)
            throws IOException {
        return getInstance(dir, pieceInfo.getFileInfo()).getPiece(pieceInfo);
    }

    /**
     * Writes a piece of data.
     * 
     * @param piece
     *            The piece of data.
     * @return {@code true} if an only if the file is complete.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws NullPointerException
     *             if {@code piece == null}.
     */
    private synchronized boolean putPiece(final Piece piece) throws IOException {
        raf.seek(piece.getOffset());
        raf.write(piece.getData());

        final int index = piece.getIndex();

        if (!indexes.get(index)) {
            indexes.set(index);
            pieceCount--;
        }

        return 0 >= pieceCount;
    }

    /**
     * Returns a piece of data.
     * 
     * @param pieceInfo
     *            Information on the piece of data.
     * @throws IOException
     *             if an I/O error occurs.
     */
    private synchronized Piece getPiece(final PieceInfo pieceInfo)
            throws IOException {
        raf.seek(pieceInfo.getOffset());
        final byte[] data = new byte[pieceInfo.getSize()];
        raf.read(data);
        return new Piece(pieceInfo, data);
    }

    /**
     * Closes this instance.
     * 
     * @throws IOException
     *             if an I/O error occurs.
     */
    private void close() throws IOException {
        raf.close();
    }
}
