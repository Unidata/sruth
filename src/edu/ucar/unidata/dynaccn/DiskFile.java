/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * A data-file that resides on a disk.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class DiskFile {
    /**
     * Information on the file.
     */
    private final FileInfo         fileInfo;
    /**
     * The associated I/O random-access-file.
     */
    private final RandomAccessFile raf;

    /**
     * Constructs from file-information.
     * 
     * @param fileInfo
     *            The file-information.
     * @param forWriting
     *            Whether or not the file should be opened for writing.
     * @throws FileNotFoundException
     *             if the file doesn't exist and can't be created.
     * @throws NullPointerException
     *             if {@code fileInfo == null}.
     */
    DiskFile(final FileInfo fileInfo, final boolean forWriting)
            throws FileNotFoundException {
        raf = new RandomAccessFile(fileInfo.getPath(), forWriting
                ? "rwd"
                : "r");

        this.fileInfo = fileInfo;
    }

    /**
     * Returns the file-piece associated with a piece-index.
     * 
     * @param index
     *            The piece-index.
     * @return The associated piece of the file.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws IllegalArgumentException
     *             if {@code index} lies outside the valid range.
     */
    synchronized Piece getPiece(final long index) throws IOException {
        raf.seek(fileInfo.getOffset(index));

        final byte[] data = new byte[fileInfo.getSize(index)];
        raf.read(data);

        return new Piece(new PieceInfo(fileInfo, index), data);
    }

    /**
     * Writes the given file-piece.
     * 
     * @param piece
     *            The file-piece to be written.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws IllegalArgumentException
     *             if the file-piece doesn't belong to this instance.
     * @throws NullPointerException
     *             if {@code piece == null}.
     */
    synchronized void putPiece(final Piece piece) throws IOException {
        fileInfo.vet(piece.getInfo());
        raf.seek(fileInfo.getOffset(piece.getIndex()));
        raf.write(piece.getData());
    }

    /**
     * Closes this instance.
     * 
     * @throws IOException
     *             if an I/O error occurs.
     */
    synchronized void close() throws IOException {
        raf.close();
    }
}
