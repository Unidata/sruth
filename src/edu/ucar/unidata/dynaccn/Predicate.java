/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

/**
 * A disjunction of filters for selecting data for a peer.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class Predicate {
    /**
     * The filters.
     */
    private final Filter[] filters;

    /**
     * Constructs from an array of filters.
     * 
     * @param filters
     *            The array of filters.
     * @throws NullPointerException
     *             if {@code filters == null}.
     */
    Predicate(final Filter[] filters) {
        if (null == filters) {
            throw new NullPointerException();
        }

        this.filters = filters;
    }

    /**
     * Indicates if a file satisfies this predicate.
     * 
     * @param fileInfo
     *            A description of the file.
     * @return {@code true} if and only if the file satisfies this predicate.
     * @throws NullPointerException
     *             if {@code fileInfo == null}.
     */
    boolean satisfiedBy(final FileInfo fileInfo) {
        for (final Filter filter : filters) {
            if (filter.satisfiedBy(fileInfo)) {
                return true;
            }
        }
        return false;
    }
}
