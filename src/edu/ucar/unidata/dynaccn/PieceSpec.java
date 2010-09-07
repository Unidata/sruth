/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.InvalidObjectException;
import java.nio.file.Path;
import java.util.Iterator;

/**
 * A data-specification comprising a single piece of data in a file.
 * 
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
     * Returns the relative pathname of this instance.
     * 
     * @return This instance's relative pathname.
     */
    @Override
    Path getPath() {
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
     * Returns the size of this piece of data in bytes.
     * 
     * @return The size, in bytes, of this piece of data.
     */
    int getSize() {
        return fileInfo.getSize(index);
    }

    @Override
    public PieceSpecSet merge(final PieceSpecSet specs) {
        return specs.merge(this);
    }

    @Override
    public PieceSpecSet merge(final MultiFilePieceSpecs specs) {
        return specs.merge(this);
    }

    @Override
    public PieceSpecSet merge(final FilePieceSpecs specs) {
        return specs.merge(this);
    }

    @Override
    public PieceSpecSet merge(final PieceSpec that) {
        if (fileInfo.equals(that.fileInfo)) {
            if (index == that.index) {
                return this;
            }
            return new FilePieceSpecs(fileInfo).merge(this).merge(that);
        }
        return new MultiFilePieceSpecs(this).merge(that);
    }

    @Override
    public boolean isEmpty() {
        return false;
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
