/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.InvalidObjectException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.prefs.Preferences;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    private static final long   serialVersionUID     = 1L;
    /**
     * The default size of a canonical piece of data.
     */
    private static final int    PIECE_SIZE           = (1 << 17);   // 131072
    /**
     * The key of the time-to-live preference, {@value} .
     */
    public static final String  TIME_TO_LIVE_KEY     = "timeToLive";
    /**
     * The default time-to-live. Set from the {@link #TIME_TO_LIVE_KEY}
     * preference.
     */
    public static final int     TIME_TO_LIVE;
    /**
     * The default time-to-live if the time-to-live preference,
     * {@link #TIME_TO_LIVE_KEY}, can't be obtained. {@value} seconds.
     */
    public static final int     DEFAULT_TIME_TO_LIVE = 3600;
    /**
     * The file identifier.
     * 
     * @serial
     */
    private final FileId        fileId;
    /**
     * The size of the file in bytes.
     * 
     * @serial
     */
    private final long          fileSize;
    /**
     * The size of piece of the file in bytes.
     * 
     * @serial
     */
    private final int           pieceSize;
    /**
     * The last valid piece-index.
     */
    private final transient int lastIndex;
    /**
     * The time-to-live in seconds.
     * 
     * @serial
     */
    private final int           timeToLive;

    static {
        final Preferences prefs = Preferences
                .userNodeForPackage(FileInfo.class);
        TIME_TO_LIVE = prefs.getInt(TIME_TO_LIVE_KEY, DEFAULT_TIME_TO_LIVE);
        if (TIME_TO_LIVE <= 0) {
            throw new IllegalArgumentException("Invalid \"" + TIME_TO_LIVE_KEY
                    + "\" preference: " + TIME_TO_LIVE);
        }
    }

    /**
     * Constructs from information on the file. The size of the data-pieces will
     * be the default piece size, {@link #getDefaultPieceSize()}, and the
     * time-to-live attribute will be the default, {@link #TIME_TO_LIVE}.
     * 
     * @param fileId
     *            The file identifier.
     * @param fileSize
     *            The size of the file in bytes.
     * @throws NullPointerException
     *             if {@code fileId} is {@code null}.
     * @throws IllegalArgumentException
     *             if {@code fileSize} is less than zero.
     * @throws IllegalArgumentException
     *             if {@code fileSize > 0}.
     */
    FileInfo(final FileId fileId, final long fileSize) {
        this(fileId, fileSize, PIECE_SIZE);
    }

    /**
     * Constructs from information on the file. The time-to-live attribute will
     * be the default, {@link #TIME_TO_LIVE}.
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
    FileInfo(final FileId fileId, final long fileSize, final int pieceSize) {
        this(fileId, fileSize, pieceSize, TIME_TO_LIVE);
    }

    /**
     * Constructs from information on the file.
     * 
     * @param fileId
     *            The file identifier.
     * @param fileSize
     *            The size of the file in bytes.
     * @param pieceSize
     *            The size of a piece of the file in bytes.
     * @param timeToLive
     *            The time for the file to live in seconds. A value of
     *            {@code -1} means indefinitely.
     * @throws NullPointerException
     *             if {@code fileId} is {@code null}.
     * @throws IllegalArgumentException
     *             if {@code fileSize} is less than zero.
     * @throws IllegalArgumentException
     *             if {@code fileSize > 0 && pieceSize <= 0}.
     * @throws IllegalArgumentException
     *             if {@code timeToLive <= 0}.
     */
    FileInfo(final FileId fileId, final long fileSize, int pieceSize,
            final int timeToLive) {
        if (null == fileId) {
            throw new NullPointerException();
        }
        if (0 > fileSize) {
            throw new IllegalArgumentException("Invalid file-size: " + fileSize);
        }
        if (0 == fileSize) {
            lastIndex = 0;
            pieceSize = 0;
        }
        else {
            if (pieceSize <= 0) {
                throw new IllegalArgumentException("Invalid piece-size: "
                        + pieceSize);
            }
            lastIndex = (int) ((fileSize - 1) / pieceSize);
        }
        if (timeToLive < -1 || timeToLive == 0) {
            throw new IllegalArgumentException("Invalid time-to-live: "
                    + timeToLive);
        }
        this.fileId = fileId;
        this.fileSize = fileSize;
        this.pieceSize = pieceSize;
        this.timeToLive = timeToLive;
    }

    /**
     * Returns the default size of a canonical piece of data.
     * 
     * @return The default size of a canonical piece of data.
     */
    static int getDefaultPieceSize() {
        return PIECE_SIZE;
    }

    /**
     * Returns the size, in bytes, of all pieces of data (except, possibly, the
     * last).
     * 
     * @return the size, in bytes, of all pieces of data (except, possibly, the
     *         last).
     */
    int getPieceSize() {
        return pieceSize;
    }

    /**
     * Returns the size, in bytes, of the last file-piece.
     * 
     * @return The size of the last file-piece.
     */
    private int lastSize() {
        return (0 == pieceSize)
                ? 0
                : (int) ((fileSize - 1) % pieceSize) + 1;
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
     * Returns the file identifier.
     * 
     * @return The file identifier.
     */
    FileId getFileId() {
        return fileId;
    }

    /**
     * Returns the pathname associated with the file.
     * 
     * @return The pathname associated with the file.
     */
    ArchivePath getPath() {
        return fileId.getPath();
    }

    /**
     * Returns the time associated with the file.
     * 
     * @return the time associated with the file.
     */
    ArchiveTime getTime() {
        return fileId.getTime();
    }

    /**
     * Returns the absolute pathname of the file given the absolute pathname of
     * the root directory of the archive.
     * 
     * @param rootDir
     *            The absolute pathname of the root directory of the archive.
     * @return the absolute pathname of the file given the absolute pathname of
     *         the root directory of the archive.
     */
    Path getAbsolutePath(final Path rootDir) {
        return fileId.getAbsolutePath(rootDir);
    }

    /**
     * Returns the size of the file in bytes.
     * 
     * @return the size of the file in bytes.
     */
    long getSize() {
        return fileSize;
    }

    /**
     * Returns the size of a file-piece in bytes.
     * 
     * @param index
     *            The index of the file-piece.
     * @throws IllegalArgumentException
     *             if {@code index} lies outside the valid range.
     * @return the size of the file-piece in bytes.
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
    int getPieceCount() {
        return lastIndex + 1;
    }

    /**
     * Returns the time-to-live attribute, in seconds.
     * 
     * @return the time-to-live attribute, in seconds.
     */
    int getTimeToLive() {
        return timeToLive;
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
    void vet(final PieceSpec pieceSpec) {
        if (!equals(pieceSpec.getFileInfo())) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Returns an iterator over the piece-informations of this instance.
     * 
     * @return An iterator over the piece-informations of this instance.
     */
    Iterator<PieceSpec> getPieceInfoIterator() {
        class PieceInfoIterator implements Iterator<PieceSpec> {
            /**
             * The index of the next piece to be returned.
             */
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index <= lastIndex;
            }

            @Override
            public PieceSpec next() {
                return new PieceSpec(FileInfo.this, index++);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        }

        return new PieceInfoIterator();
    }

    /**
     * Indicates if this instance matches a pattern.
     * 
     * @param pattern
     *            The pattern to be matched.
     * @return {@code true} if and only if this instance matches the pattern.
     */
    boolean matches(final Pattern pattern) {
        return fileId.matches(pattern);
    }

    /**
     * Returns a {@link Matcher} based on this instance and a pattern.
     * 
     * @param pattern
     *            The pattern.
     * @return The corresponding {@link Matcher}.
     */
    Matcher matcher(final Pattern pattern) {
        return fileId.matcher(pattern);
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
        result = prime * result + timeToLive;
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
        if (timeToLive != other.timeToLive) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " [filedId=" + fileId
                + ",fileSize=" + fileSize + ",pieceSize=" + pieceSize
                + ",timeToLive=" + timeToLive + "]";
    }

    private Object readResolve() throws InvalidObjectException {
        try {
            return new FileInfo(fileId, fileSize, pieceSize, timeToLive);
        }
        catch (final Exception e) {
            throw (InvalidObjectException) new InvalidObjectException(
                    "Read invalid " + getClass().getSimpleName()).initCause(e);
        }
    }
}
