/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.InvalidObjectException;
import java.util.BitSet;

import net.jcip.annotations.GuardedBy;

/**
 * A finite-size bit-set whose bits are not all set.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class PartialBitSet extends FiniteBitSet {
    /**
     * The serial version ID.
     */
    private static final long serialVersionUID = 1L;
    /**
     * The tracking bit-set.
     */
    @GuardedBy("this")
    private BitSet            bitSet;
    /**
     * The number of set bits.
     */
    @GuardedBy("this")
    private int               setCount;

    /**
     * Constructs from the number of bits. All bits are initially unset.
     * 
     * @param size
     *            The number of bits.
     * @throws IllegalArgumentException
     *             if {@code size < 0}.
     */
    protected PartialBitSet(final int size) {
        super(size);
        synchronized (this) {
            bitSet = new BitSet(); // size omitted to conserve space
            setCount = 0;
        }
    }

    /**
     * Constructs an instance with all bits set except for one.
     * 
     * @param size
     *            The number of bits.
     * @param index
     *            The 0-based index of the clear bit.
     */
    PartialBitSet(final int size, final int index) {
        super(size);
        synchronized (this) {
            bitSet = new BitSet(size);
            bitSet.set(0, size - 1);
            setCount = size - 1;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.ucar.unidata.sruth.FiniteBitSet#setBit(int)
     */
    @Override
    synchronized FiniteBitSet setBit(final int index) {
        vetIndex(index);
        FiniteBitSet result;
        if (bitSet.get(index)) {
            // already set
            result = this;
        }
        else {
            if (setCount == size - 1) {
                result = new CompleteBitSet(size);
            }
            else {
                bitSet.set(index);
                setCount++;
                result = this;
            }
        }
        return result;
    }

    @Override
    synchronized FiniteBitSet clearBit(final int index) {
        vetIndex(index);
        if (bitSet.get(index)) {
            bitSet.clear(index);
            setCount--;
        }
        return this;
    }

    @Override
    FiniteBitSet setAll() {
        return new CompleteBitSet(size);
    }

    @Override
    synchronized boolean isSet(final int index) {
        return bitSet.get(index);
    }

    @Override
    synchronized boolean areAllSet() {
        return size == setCount;
    }

    /**
     * Returns the next set bit.
     * 
     * @param i
     *            Index of the bit from which to start. If {@code isSet(i)},
     *            then {@code i} will be returned.
     * @return Index of the next set bit on or after bit {@code i} or {@code -1}
     *         if there are no more set bits.
     */
    @Override
    synchronized int nextSetBit(final int i) {
        return bitSet.nextSetBit(i);
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.ucar.unidata.sruth.FiniteBitSet#getSetCount()
     */
    @Override
    synchronized int getSetCount() {
        return setCount;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.ucar.unidata.sruth.FiniteBitSet#merge(edu.ucar.unidata.sruth.
     * FiniteBitSet)
     */
    @Override
    FiniteBitSet merge(final FiniteBitSet that) {
        return that.merge(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.ucar.unidata.sruth.FiniteBitSet#merge(edu.ucar.unidata.sruth.
     * PartialBitSet)
     */
    @Override
    protected FiniteBitSet merge(final PartialBitSet that) {
        if (this == that) {
            return this;
        }
        vetForMerger(that);
        FiniteBitSet result;
        PartialBitSet set1;
        PartialBitSet set2;
        if (System.identityHashCode(this) < System.identityHashCode(that)) {
            set1 = this;
            set2 = that;
        }
        else {
            set1 = that;
            set2 = this;
        }
        synchronized (set1) { // always locked
            synchronized (set2) { // in same order
                final BitSet newBitSet = (BitSet) bitSet.clone();
                final int newSetCount = newBitSet.cardinality();
                if (size == newSetCount) {
                    result = new CompleteBitSet(size);
                }
                else {
                    bitSet = newBitSet;
                    setCount = newSetCount;
                    result = this;
                }
            }
        }
        return result;
    }

    @Override
    public synchronized PartialBitSet clone() {
        final PartialBitSet clone = (PartialBitSet) super.clone();
        synchronized (clone) {
            clone.bitSet.and(bitSet);
        }
        return clone;
    }

    @Override
    public synchronized String toString() {
        return getClass().getSimpleName() + "{size=" + size + ", setCount="
                + setCount + "}";
    }

    private Object readResolve() throws InvalidObjectException {
        if (0 > size) {
            throw new InvalidObjectException("Invalid size: " + size);
        }
        if (0 > setCount) {
            throw new InvalidObjectException("Invalid setCount: " + size);
        }
        if (size <= setCount) {
            throw new InvalidObjectException("Size (" + size
                    + ") <= setCount (" + setCount + ")");
        }
        if (bitSet.cardinality() != setCount) {
            throw new InvalidObjectException("Cardinality ("
                    + bitSet.cardinality() + ") != setCount (" + setCount + ")");
        }
        return this;
    }
}
