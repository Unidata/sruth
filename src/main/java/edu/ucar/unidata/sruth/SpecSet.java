package edu.ucar.unidata.sruth;

import net.jcip.annotations.GuardedBy;

/**
 * A set of data-piece specifications.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class SpecSet {
    /**
     * The mutable set of piece-specifications
     */
    @GuardedBy("this")
    private PieceSpecSetIface specs = EmptyPieceSpecSet.INSTANCE;
    /**
     * The number of piece-specifications
     */
    @GuardedBy("this")
    private long              size  = 0;

    /**
     * Adds a piece-specification.
     * 
     * @param spec
     *            The piece-specification to be added
     * @return {@code true} if and only if this instance didn't contain the
     *         given piece-specification
     */
    synchronized boolean add(final PieceSpec spec) {
        final boolean wasAdded = !specs.contains(spec);
        if (wasAdded) {
            specs = specs.merge(spec);
            size++;
        }
        return wasAdded;
    }

    /**
     * Removes a piece-specification.
     * 
     * @param spec
     *            The piece-specification to be removed
     * @return {@code true} if and only if this instance contained the given
     *         piece-specification.
     */
    synchronized boolean remove(final PieceSpec spec) {
        final boolean wasRemoved = specs.contains(spec);
        if (wasRemoved) {
            specs = specs.remove(spec);
            size--;
        }
        return wasRemoved;
    }

    /**
     * Returns the number of piece-specifications in this instance.
     * 
     * @return the number of piece-specifications in this instance.
     */
    synchronized long size() {
        return size;
    }

    /**
     * Returns a copy of the underlying, mutable, set of data-piece
     * specifications.
     * 
     * @return a copy of the underlying, mutable, set of data-piece
     *         specifications
     */
    synchronized PieceSpecSetIface getSet() {
        return specs.clone();
    }
}