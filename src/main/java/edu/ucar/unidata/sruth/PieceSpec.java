/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.InvalidObjectException;
import java.util.Iterator;

/**
 * A data-specification comprising a single piece of data in a file.
 * <p>
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class PieceSpec extends FilePieceSpecSet {
    /**
     * The serial version identifier.
     */
    private static final long serialVersionUID = 1L;
    /**
     * The piece index.
     * 
     * @serial
     */
    private final int         index;

    /**
     * Constructs from a file identifier and a piece index.
     * 
     * @param fileInfo
     *            Information on the file.
     * @param index
     *            The piece index.
     * @throws IllegalArgumentException
     *             if {@code index} lies outside the valid range.
     * @throws NullPointerException
     *             if {@code fileInfo == null}.
     */
    PieceSpec(final FileInfo fileInfo, final int index) {
        super(fileInfo);
        fileInfo.vet(index);
        this.index = index;
    }

    /**
     * Vets an array of data.
     * 
     * @param data
     *            The data to be vetted.
     * @throws IllegalArgumentException
     *             if {@code data} has the wrong number of elements.
     * @throws NullPointerException
     *             if {@code data == null}.
     */
    void vet(final byte[] data) {
        fileInfo.vet(index, data);
    }

    /**
     * Returns the piece-index of this instance.
     * 
     * @return The piece-index of this instance.
     */
    int getIndex() {
        return index;
    }

    /**
     * Returns the archive pathname of this instance.
     * 
     * @return This instance's archive pathname.
     */
    @Override
    ArchivePath getArchivePath() {
        return fileInfo.getPath();
    }

    /**
     * Returns the number of bytes from the beginning of the file to the start
     * of this piece.
     * 
     * @return The offset, in bytes, to the start of this piece.
     */
    long getOffset() {
        return fileInfo.getOffset(index);
    }

    /**
     * Returns the size of this piece of data in octets.
     * 
     * @return The size, in octets, of this piece of data.
     */
    int getSize() {
        return fileInfo.getSize(index);
    }

    /**
     * Returns the time-to-live, in seconds, of the associated file.
     * 
     * @return The time-to-live, in seconds, of the associated file.
     */
    int getTimeToLive() {
        return fileInfo.getTimeToLive();
    }

    @Override
    public PieceSpecSetIface merge(final PieceSpecSetIface specs) {
        return specs.merge(this);
    }

    @Override
    public PieceSpecSetIface merge(final PieceSpecSet specs) {
        return specs.merge(this);
    }

    @Override
    public PieceSpecSetIface merge(final FilePieceSpecs specs) {
        return specs.merge(this);
    }

    @Override
    public PieceSpecSetIface merge(final PieceSpec that) {
        if (getFileId().equals(that.getFileId())) {
            if (index == that.index) {
                return this;
            }
            return new FilePieceSpecs(fileInfo).merge(this).merge(that);
        }
        return new PieceSpecSet(this).merge(that);
    }

    @Override
    public PieceSpecSetIface remove(final PieceSpec spec) {
        return equals(spec)
                ? EmptyPieceSpecSet.INSTANCE
                : this;
    }

    @Override
    public boolean contains(final PieceSpec spec) {
        return equals(spec);
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public PieceSpec clone() {
        return this; // because instances are immutable
    }

    @Override
    public Iterator<PieceSpec> iterator() {
        return new SimpleIterator<PieceSpec>(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + index;
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
        if (!super.equals(obj)) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final PieceSpec other = (PieceSpec) obj;
        if (index != other.index) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{fileInfo=" + fileInfo
                + ", index=" + index + "}";
    }

    private Object readResolve() throws InvalidObjectException {
        try {
            return new PieceSpec(fileInfo, index);
        }
        catch (final Exception e) {
            throw (InvalidObjectException) new InvalidObjectException(
                    "Read invalid " + getClass().getSimpleName()).initCause(e);
        }
    }
}
