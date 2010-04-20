/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.util.Set;
import java.util.TreeSet;

/**
 * A disjunction of filters for selecting data for a peer.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class Predicate {
    /**
     * The filters.
     */
    private final Set<Filter> filters = new TreeSet<Filter>();

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

        for (final Filter filter : filters) {
            this.filters.add(filter);
        }
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
    synchronized boolean satisfiedBy(final FileInfo fileInfo) {
        for (final Filter filter : filters) {
            if (filter.satisfiedBy(fileInfo)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Removes a specification of a file from this instance, if possible. If a
     * filter of this predicate is satisfied by the given file specification and
     * only by that specification, then that filter will be removed from this
     * instance.
     * 
     * @param fileInfo
     *            Information on the file to be removed.
     * @see {@link Filter#exactlySpecifies(FileInfo)}
     */
    synchronized void removeIfPossible(final FileInfo fileInfo) {
        for (final Filter filter : filters) {
            if (filter.exactlySpecifies(fileInfo)) {
                filters.remove(filter);
                break;
            }
        }
    }

    /**
     * Indicates if this instance contains no filters.
     * 
     * @return {@code true} if and only if this instance contains no filters.
     */
    synchronized boolean isEmpty() {
        return filters.isEmpty();
    }
}