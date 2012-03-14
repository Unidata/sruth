/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.InvalidObjectException;
import java.util.Iterator;

import net.jcip.annotations.ThreadSafe;

/**
 * A data-specification comprising multiple pieces of data in a file.
 * 
 * Instances are mutable and thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class FilePieceSpecs extends FilePieceSpecSet {
    /**
     * The serial version ID.
     */
    private static final long serialVersionUID = 1L;
    /**
     * A bit-set that specifies the piece indexes.
     */
    private FiniteBitSet      indexes;

    /**
     * Constructs from information on a file. Initially, no pieces are
     * specified.
     * 
     * @param fileInfo
     *            Information on the file.
     * @throws NullPointerException
     *             if {@code fileInfo == null}.
     */
    protected FilePieceSpecs(final FileInfo fileInfo) {
        this(fileInfo, false);
    }

    /**
     * Constructs from information on a file and whether it contains all pieces.
     * 
     * @param fileInfo
     *            Information on the file.
     * @param allPieces
     *            Whether or not all pieces should be specified.
     * @throws NullPointerException
     *             if {@code fileInfo == null}.
     */
    FilePieceSpecs(final FileInfo fileInfo, final boolean allPieces) {
        super(fileInfo);
        indexes = FiniteBitSet.newInstance(fileInfo.getPieceCount(), allPieces);
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
    public PieceSpecSet merge(final FilePieceSpecs that) {
        if (this == that) {
            return this;
        }
        PieceSpecSet result;
        if (!getFileId().equals(that.getFileId())) {
            result = new MultiFilePieceSpecs(this).merge(that);
        }
        else {
            vet(that.getFileInfo());
            FilePieceSpecs o1, o2;
            if (System.identityHashCode(this) < System.identityHashCode(that)) {
                o1 = this;
                o2 = that;
            }
            else {
                o1 = that;
                o2 = this;
            }
            synchronized (o1) {
                synchronized (o2) {
                    if (indexes.areAllSet()) {
                        result = this;
                    }
                    else if (that.indexes.areAllSet()) {
                        result = that;
                    }
                    else {
                        final FiniteBitSet newIndexes = indexes
                                .merge(that.indexes);
                        if (newIndexes.areAllSet()) {
                            result = new FilePieceSpecs(fileInfo, true);
                        }
                        else {
                            indexes = newIndexes;
                            result = this;
                        }
                    }
                }
            }
        }
        return result;
    }

    /**
     * Verifies that two file-informations are the same.
     * 
     * @param expected
     *            The expected file-information.
     * @param actual
     *            The actual file-information.
     * @throws IllegalArgumentException
     *             if the two file-informations are not equal.
     * @throws NullPointerException
     *             if {@code expected == null}.
     */
    private void vet(final FileInfo actual) {
        final FileInfo expected = getFileInfo();
        if (!expected.equals(actual)) {
            throw new IllegalArgumentException("expected=" + expected
                    + ", actual=" + actual);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.ucar.unidata.sruth.SpecThing#merge(edu.ucar.unidata.sruth.PieceSpec )
     */
    @Override
    public synchronized PieceSpecSet merge(final PieceSpec that) {
        if (getFileId().equals(that.getFileId())) {
            vet(that.getFileInfo());
            indexes = indexes.setBit(that.getIndex());
            return this;
        }
        return new MultiFilePieceSpecs(this).merge(that);
    }

    @Override
    public synchronized boolean isEmpty() {
        return indexes.getSetCount() == 0;
    }

    @Override
    public Iterator<PieceSpec> iterator() {
        return new SimpleIterator<PieceSpec>() {
            private int index = 0;

            @Override
            protected PieceSpec getNext() {
                synchronized (FilePieceSpecs.this) {
                    index = indexes.nextSetBit(index);
                    return (-1 == index)
                            ? null
                            : new PieceSpec(fileInfo, index++);
                }
            }
        };
    }

    @Override
    public synchronized String toString() {
        return getClass().getSimpleName() + "{fileInfo=" + fileInfo
                + ",indexes=" + indexes + "}";
    }

    private Object readResolve() throws InvalidObjectException {
        if (fileInfo.getPieceCount() != indexes.getSize()) {
            throw new InvalidObjectException(toString());
        }
        return this;
    }
}
