/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

/**
 * A finite-size bit-set with all bits set.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class CompleteBitSet extends FiniteBitSet {
    /**
     * The serial version ID.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Constructs from the number of bits. All bits are set.
     * 
     * @param size
     *            The number of bits.
     * @throws IllegalArgumentException
     *             if {@code size < 0}.
     */
    protected CompleteBitSet(final int size) {
        super(size);
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.ucar.unidata.sruth.FiniteBitSet#getSetCount()
     */
    @Override
    int getSetCount() {
        return size;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.ucar.unidata.sruth.FiniteBitSet#setBit(int)
     */
    @Override
    FiniteBitSet setBit(final int index) {
        vetIndex(index);
        return this;
    }

    @Override
    FiniteBitSet setAll() {
        return this;
    }

    @Override
    boolean isSet(final int index) {
        vetIndex(index);
        return true;
    }

    @Override
    boolean areAllSet() {
        return true;
    }

    @Override
    int nextSetBit(final int i) {
        return size > i
                ? i
                : -1;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.ucar.unidata.sruth.FiniteBitSet#merge(edu.ucar.unidata.sruth.
     * FiniteBitSet)
     */
    @Override
    FiniteBitSet merge(final FiniteBitSet that) {
        vetForMerger(that);
        return this;
    }

    @Override
    protected FiniteBitSet merge(final PartialBitSet that) {
        vetForMerger(that);
        return this;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{size=" + size + "}";
    }

    private Object readResolve() {
        return new CompleteBitSet(size);
    }
}
