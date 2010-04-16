/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.File;
import java.io.InvalidObjectException;
import java.io.Serializable;
import java.util.Iterator;

/**
 * Information about a file.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class FileInfo implements Serializable {
    /**
     * The serial version identifier.
     */
    private static final long    serialVersionUID = 1L;
    /**
     * The file identifier.
     */
    private final FileId         fileId;
    /**
     * The size of the file in bytes.
     */
    private final long           fileSize;
    /**
     * The size of piece of the file in bytes.
     */
    private final int            pieceSize;
    /**
     * The last valid piece-index.
     */
    private final transient long lastIndex;

    /**
     * Constructs from information on the file.
     * 
     * @param fileId
     *            The file identifier.
     * @param fileSize
     *            The size of the file in bytes.
     * @param pieceSize
     *            The size of a piece of the file in bytes.
     * @throws NullPointerException
     *             if {@code fileId} is {@code null}.
     * @throws IllegalArgumentException
     *             if {@code fileSize} is less than zero.
     * @throws IllegalArgumentException
     *             if {@code fileSize > 0 && pieceSize <= 0}.
     */
    FileInfo(final FileId fileId, final long fileSize, int pieceSize) {
        if (null == fileId) {
            throw new NullPointerException();
        }
        if (0 > fileSize) {
            throw new IllegalArgumentException("Invalid file-size: " + fileSize);
        }
        if (0 == fileSize) {
            lastIndex = -1;
            pieceSize = 1;
        }
        else {
            if (pieceSize <= 0) {
                throw new IllegalArgumentException("Invalid piece-size: "
                        + pieceSize);
            }
            lastIndex = (fileSize - 1) / pieceSize;
        }
        this.fileId = fileId;
        this.fileSize = fileSize;
        this.pieceSize = pieceSize;
    }

    /**
     * Returns the size, in bytes, of the last file-piece.
     * 
     * @return The size of the last file-piece.
     */
    private int lastSize() {
        return (int) ((fileSize - 1) % pieceSize) + 1;
    }

    /**
     * Vets a piece-index.
     * 
     * @param index
     *            The piece-index to be vetted.
     * @throws IllegalArgumentException
     *             if {@code index} lies outside the valid range.
     */
    void vet(final long index) {
        if (0 > index || lastIndex < index) {
            throw new IllegalArgumentException(
                    "Index lies outside valid range for " + this + ": " + index);
        }
    }

    /**
     * Vets a piece-index and the associated data.
     * 
     * @param index
     *            The piece-index to be vetted.
     * @param data
     *            The data to be vetted.
     * @throws IllegalArgumentException
     *             if {@code index} lies outside the valid range.
     * @throws IllegalArgumentException
     *             if {@code data} has the wrong number of elements.
     * @throws NullPointerException
     *             if {@code data == null}.
     */
    void vet(final long index, final byte[] data) {
        vet(index);

        final long size = (lastIndex == index)
                ? lastSize()
                : pieceSize;

        if (data.length != size) {
            throw new IllegalArgumentException(
                    "Invalid data-size.  Should have " + size + " bytes; has "
                            + data.length);
        }
    }

    /**
     * Returns the pathname associated with this instance.
     * 
     * @return The associated pathname.
     */
    File getPath() {
        return fileId.getFile();
    }

    /**
     * Returns the size of a file-piece in bytes.
     * 
     * @param index
     *            The index of the file-piece.
     * @throws IllegalArgumentException
     *             if {@code index} lies outside the valid range.
     * @param index
     * @return
     */
    int getSize(final long index) {
        return lastIndex == index
                ? lastSize()
                : pieceSize;
    }

    /**
     * Returns the number of data-pieces in this instance.
     * 
     * @return The number of data-pieces.
     */
    long getPieceCount() {
        return lastIndex + 1;
    }

    /**
     * Returns the offset, in bytes, to the start of the file-piece with the
     * given piece-index.
     * 
     * @param index
     *            The index of the file-piece.
     * @return The offset, in bytes, to the start of the file-piece.
     * @throws IllegalArgumentException
     *             if {@code index} lies outside the valid range.
     */
    long getOffset(final long index) {
        vet(index);
        return pieceSize * index;
    }

    /**
     * Vets the information on a file-piece.
     * 
     * @param info
     *            The piece-information to be vetted.
     * @throws IllegalArgumentException
     *             if {@code info} is incompatible with this instance.
     */
    void vet(final PieceInfo pieceInfo) {
        if (!equals(pieceInfo.getFileInfo())) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Returns an iterator over the piece-informations of this instance.
     * 
     * @return An iterator over the piece-informations of this instance.
     */
    Iterator<PieceInfo> getPieceInfoIterator() {
        class PieceInfoIterator implements Iterator<PieceInfo> {
            /**
             * The index of the next piece to be returned.
             */
            private long index = 0;

            @Override
            public boolean hasNext() {
                return index <= lastIndex;
            }

            @Override
            public PieceInfo next() {
                return new PieceInfo(FileInfo.this, index++);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        }

        return new PieceInfoIterator();
    }

    /**
     * Returns the absolute abstract pathname of this instance's file resolved
     * against a given directory.
     * 
     * @param dirPath
     *            The directory against which to resolve the pathname.
     */
    File getFile(final File dirPath) {
        return fileId.getFile(dirPath);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fileId == null)
                ? 0
                : fileId.hashCode());
        result = prime * result + (int) (fileSize ^ (fileSize >>> 32));
        result = prime * result + pieceSize;
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof FileInfo)) {
            return false;
        }
        final FileInfo other = (FileInfo) obj;
        if (fileId == null) {
            if (other.fileId != null) {
                return false;
            }
        }
        else if (!fileId.equals(other.fileId)) {
            return false;
        }
        if (fileSize != other.fileSize) {
            return false;
        }
        if (pieceSize != other.pieceSize) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{filedId=" + fileId
                + ", fileSize=" + fileSize + ", pieceSize=" + pieceSize + "}";
    }

    private Object readResolve() throws InvalidObjectException {
        try {
            return new FileInfo(fileId, fileSize, pieceSize);
        }
        catch (final Exception e) {
            throw (InvalidObjectException) new InvalidObjectException(
                    "Read invalid " + getClass().getSimpleName()).initCause(e);
        }
    }

    /**
     * Returns the value of a given attribute.
     * 
     * @param attribute
     *            The attribute.
     * @return The value of the attribute in this instance or {@code null} if
     *         this instance doesn't have the attribute.
     */
    AttributeValue getAttributeValue(final Attribute attribute) {
        return fileId.getAttributeValue(attribute);
    }
}
