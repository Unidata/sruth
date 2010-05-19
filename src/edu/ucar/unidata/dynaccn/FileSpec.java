/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.InvalidObjectException;

/**
 * A data-specification comprising multiple pieces of data in a file.
 * 
 * Instances are mutable and thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class FileSpec extends PiecesSpec {
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
    protected FileSpec(final FileInfo fileInfo) {
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
    FileSpec(final FileInfo fileInfo, final boolean allPieces) {
        super(fileInfo);
        indexes = allPieces
                ? new CompleteBitSet(fileInfo.getPieceCount())
                : FiniteBitSet.newInstance(fileInfo.getPieceCount());
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.ucar.unidata.dynaccn.SpecThing#merge(edu.ucar.unidata.dynaccn.SpecThing
     * )
     */
    @Override
    public PiecesSpec merge(final PiecesSpec that) {
        return that.merge(this); // double dispatch
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.ucar.unidata.dynaccn.SpecThing#merge(edu.ucar.unidata.dynaccn.PieceSpec
     * )
     */
    @Override
    public synchronized PiecesSpec merge(final PieceSpec that) {
        vetMerger(that);
        indexes = indexes.setBit(that.getIndex());
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @seeedu.ucar.unidata.dynaccn.DataSpec#merge(edu.ucar.unidata.dynaccn.
     * FileSpec)
     */
    @Override
    public PiecesSpec merge(final FileSpec that) {
        vetMerger(that);
        FileSpec spec1;
        FileSpec spec2;
        if (System.identityHashCode(this) < System.identityHashCode(that)) {
            spec1 = this;
            spec2 = that;
        }
        else {
            spec1 = that;
            spec2 = this;
        }
        synchronized (spec1) { // always locked
            synchronized (spec2) { // in same order
                indexes = indexes.merge(that.indexes);
            }
        }
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.ucar.unidata.dynaccn.SpecThing#processYourself(edu.ucar.unidata.dynaccn
     * .SpecProcessor)
     */
    @Override
    public synchronized void processYourself(final SpecProcessor specProcessor)
            throws InterruptedException, IOException {
        for (int i = indexes.nextSetBit(0); i >= 0; i = indexes
                .nextSetBit(i + 1)) {
            specProcessor.process(new PieceSpec(fileInfo, i));
        }
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
