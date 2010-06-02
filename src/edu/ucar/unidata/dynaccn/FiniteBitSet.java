/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.Serializable;

/**
 * A finite-size set of bits.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
abstract class FiniteBitSet implements Serializable {
    /**
     * The serial version ID.
     */
    private static final long serialVersionUID = 1L;
    /**
     * The number of bits.
     */
    protected final int       size;

    /**
     * Constructs from the number of bits.
     * 
     * @param size
     *            The number of bits.
     * @throws IllegalArgumentException
     *             if {@code size < 0}.
     */
    protected FiniteBitSet(final int size) {
        if (0 > size) {
            throw new IllegalArgumentException();
        }
        this.size = size;
    }

    /**
     * Returns a new instance. Correctly handles the case where the number of
     * bits is zero. All bits are unset.
     * 
     * @param size
     *            The number of bits.
     * @return A new instance.
     */
    public static FiniteBitSet newInstance(final int size) {
        return newInstance(size, false);
    }

    /**
     * Returns a new instance. Correctly handles the case where the number of
     * bits is zero.
     * 
     * @param size
     *            The number of bits.
     * @param setAll
     *            Whether or not to set all bits.
     * @return A new instance.
     */
    static FiniteBitSet newInstance(final int size, final boolean setAll) {
        return (0 == size || setAll)
                ? new CompleteBitSet(size)
                : new PartialBitSet(size);
    }

    /**
     * Returns the number of bits.
     * 
     * @return The number of bits.
     */
    final int getSize() {
        return size;
    }

    /**
     * Vets an index.
     * 
     * @param index
     *            The index to be vetted.
     * @throws IllegalArgumentException
     *             if {@code index < 0 || index >= getSize()}.
     */
    protected final void vetIndex(final int index) {
        if (0 > index || size <= index) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Returns the result of setting a bit. The returned instance may be this
     * instance or another instance. If this instance isn't returned, then it's
     * unmodified.
     * 
     * @param index
     *            0-based index of the bit to set.
     * @return The result of setting the indicated bit.
     * @throws IllegalArgumentException
     *             if {@code index < 0 || index >= getSize()}.
     */
    abstract FiniteBitSet setBit(int index);

    /**
     * Returns the result of setting all bits. The returned instance may be this
     * instance or another instance. If this instance isn't returned, then it's
     * unmodified.
     * 
     * @return The result of setting all bits.
     */
    abstract FiniteBitSet setAll();

    /**
     * Returns the number of set bits.
     * 
     * @return The number of set bits.
     */
    abstract int getSetCount();

    /**
     * Indicates if a bit is set.
     * 
     * @param index
     *            Index of the bit.
     * @return {@code true} if and only if the given bit is set.
     * @throws IllegalArgumentException
     *             if {@code index < 0 || index >= getSize()}.
     */
    abstract boolean isSet(int index);

    /**
     * Indicates if all bits are set.
     * 
     * @return {@code true} if and only if all bits are set.
     */
    abstract boolean areAllSet();

    /**
     * Returns the next set bit.
     * 
     * @param i
     *            Index of the bit from which to start. If {@code isSet(i)},
     *            then {@code i} will be returned.
     * @return Index of the next set bit on or after bit {@code i} or {@code -1}
     *         if there are no more set bits.
     */
    abstract int nextSetBit(final int i);

    /**
     * Vets another instance for merger with this instance.
     * 
     * @param that
     *            The other instance.
     * @throws IllegalArgumentException
     *             if the instances can't be merged.
     */
    protected final void vetForMerger(final FiniteBitSet that) {
        if (size != that.size) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Consolidates this instance with another. The returned instance may be
     * this instance, or the other instance, or a new instance. Unreturned
     * instances are unmodified.
     * 
     * @param that
     *            The other instance.
     */
    abstract FiniteBitSet merge(FiniteBitSet that);

    /**
     * Consolidates this instance with a {@link PartialBitSet}. The returned
     * instance may be this instance, or the other instance, or a new instance.
     * Unreturned instances are unmodified.
     * 
     * @param that
     * @return
     */
    protected abstract FiniteBitSet merge(PartialBitSet that);

    /**
     * Consolidates this instance with a @link CompleteBitSet}. The returned
     * instance is the other instance and this instance is unmodified.
     * 
     * @param that
     * @return
     */
    protected final FiniteBitSet merge(final CompleteBitSet that) {
        vetForMerger(that);
        return that;
    }
}
