/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

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
        PieceSpecSet result;
        if (!fileInfo.equals(that.fileInfo)) {
            result = new MultiFilePieceSpecs(this).merge(that);
        }
        else {
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

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.ucar.unidata.dynaccn.SpecThing#merge(edu.ucar.unidata.dynaccn.PieceSpec
     * )
     */
    @Override
    public synchronized PieceSpecSet merge(final PieceSpec that) {
        if (fileInfo.equals(that.getFileInfo())) {
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
