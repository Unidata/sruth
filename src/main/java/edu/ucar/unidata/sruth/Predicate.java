/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.InvalidObjectException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import net.jcip.annotations.GuardedBy;
import edu.ucar.unidata.sruth.Connection.Message;

/**
 * A disjunction of filters for selecting files.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
public class Predicate implements Iterable<Filter>, Message {
    /**
     * The serial version ID.
     */
    private static final long     serialVersionUID = 1L;
    /**
     * An instance that's satisfied by everything.
     */
    public static final Predicate EVERYTHING       = new Predicate(
                                                           Filter.EVERYTHING) {
                                                       /**
                                                        * The serial version ID.
                                                        */
                                                       private static final long serialVersionUID = 1L;

                                                       @Override
                                                       public Predicate add(
                                                               final Filter filter) {
                                                           return this;
                                                       }

                                                       @Override
                                                       synchronized boolean matches(
                                                               final FileInfo fileInfo) {
                                                           return true;
                                                       }

                                                       @Override
                                                       synchronized void removeIfPossible(
                                                               final FileInfo fileInfo) {
                                                       }

                                                       @Override
                                                       synchronized boolean matchesNothing() {
                                                           return false;
                                                       }

                                                       @Override
                                                       public String toString() {
                                                           return "EVERYTHING";
                                                       }

                                                       private Object readResolve() {
                                                           // Don't return
                                                           // "this"
                                                           return EVERYTHING;
                                                       }
                                                   };
    /**
     * An instance that's satisfied by nothing.
     */
    public static final Predicate NOTHING          = new Predicate() {
                                                       /**
                                                        * The serial version ID.
                                                        */
                                                       private static final long serialVersionUID = 1L;

                                                       @Override
                                                       public Predicate add(
                                                               final Filter filter) {
                                                           if (filter
                                                                   .equals(Filter.EVERYTHING)) {
                                                               return EVERYTHING;
                                                           }
                                                           return new Predicate(
                                                                   filter);
                                                       }

                                                       @Override
                                                       synchronized boolean matches(
                                                               final FileInfo fileInfo) {
                                                           return false;
                                                       }

                                                       @Override
                                                       synchronized void removeIfPossible(
                                                               final FileInfo fileInfo) {
                                                       }

                                                       @Override
                                                       synchronized boolean matchesNothing() {
                                                           return true;
                                                       }

                                                       @Override
                                                       public String toString() {
                                                           return "NOTHING";
                                                       }

                                                       private Object readResolve() {
                                                           // Don't return
                                                           // "this"
                                                           return NOTHING;
                                                       }
                                                   };
    /**
     * The filters.
     */
    @GuardedBy("this")
    private final Set<Filter>     filters          = new TreeSet<Filter>();

    /**
     * Constructs from nothing. The predicate will be satisfied by nothing.
     */
    public Predicate() {
    }

    /**
     * Constructs from a filter.
     * 
     * @param filter
     *            The filter.
     * @throws NullPointerException
     *             if {@code filters == null}.
     */
    private Predicate(final Filter filter) {
        if (null == filter) {
            throw new NullPointerException();
        }
        filters.add(filter);
    }

    /**
     * Adds a filter and returns the predicate that is the result of the
     * addition.
     * 
     * @param filter
     *            The filter to be added.
     * @return The predicate that results from the addition of the filter. Might
     *         differ from this instance.
     * @throws NullPointerException
     *             if {@code filter == null}.
     */
    public synchronized Predicate add(final Filter filter) {
        for (final Iterator<Filter> iter = filters.iterator(); iter.hasNext();) {
            final Filter extantFilter = iter.next();
            if (filter.includes(extantFilter) && !filter.equals(extantFilter)) {
                iter.remove();
            }
            else {
                if (extantFilter.includes(filter)) {
                    return this;
                }
            }
        }
        filters.add(filter);
        return this;
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
    synchronized boolean matches(final FileInfo fileInfo) {
        for (final Filter filter : filters) {
            if (filter.matches(fileInfo.getPath())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Indicates if a piece of data satisfies this predicate.
     * 
     * @param pieceSpec
     *            Information on the piece of data.
     * @return {@code true} if and only if the piece of data satisfies this
     *         predicate.
     * @throws NullPointerException
     *             if {@code pieceInfo == null}.
     */
    final boolean matches(final PieceSpec pieceSpec) {
        return matches(pieceSpec.getFileInfo());
    }

    /**
     * Removes a specification of a file from this instance, if possible. If a
     * filter of this predicate is satisfied by the given file specification and
     * only by that specification, then that filter will be removed from this
     * instance.
     * 
     * @param fileInfo
     *            Information on the file to be removed.
     * @see {@link Filter#matchesOnly(FileInfo)}
     */
    synchronized void removeIfPossible(final FileInfo fileInfo) {
        for (final Iterator<Filter> iter = filters.iterator(); iter.hasNext();) {
            if (iter.next().matchesOnly(fileInfo.getPath())) {
                iter.remove();
                break;
            }
        }
    }

    /**
     * Indicates if nothing can satisfy this instance.
     * 
     * @return {@code true} if and only if nothing can satisfy this instance.
     */
    synchronized boolean matchesNothing() {
        return filters.isEmpty();
    }

    /**
     * Returns the number of filters in this instance.
     * 
     * @return The number of filters in this instance.
     */
    synchronized int getFilterCount() {
        return filters.size();
    }

    @Override
    public synchronized Iterator<Filter> iterator() {
        return filters.iterator();
    }

    /**
     * Returns the set of filters in this instance. The returned set is not
     * backed by this instance.
     * 
     * @return The set of filters in this instance.
     */
    Set<Filter> getFilters() {
        return new TreeSet<Filter>(filters);
    }

    /**
     * Returns the filter of this instance that includes a given filter.
     * 
     * @param targetFilter
     *            The filter to be included by the returned filter of this
     *            instance.
     * @return A filter of this instance that includes the given filter or
     *         {@link Filter#NOTHING} if no such filter exists.
     */
    Filter getIncludingFilter(final Filter targetFilter) {
        for (final Filter filter : filters) {
            if (filter.includes(targetFilter)) {
                return filter;
            }
        }
        return Filter.NOTHING;
    }

    /**
     * Indicates if this instance is more inclusive than another instance, i.e.,
     * whether or not this instance's set of matches is a proper superset of the
     * other instance's.
     * 
     * @param that
     *            The other instance.
     * @return {@code true} if and only if this instance is more inclusive than
     *         the other instance.
     * @throws NullPointerException
     *             if {@code that == null}.
     */
    boolean isMoreInclusiveThan(final Predicate that) {
        for (final Filter thisFilter : filters) {
            for (final Filter thatFilter : that.filters) {
                if (thatFilter.includes(thisFilter)
                        && !thisFilter.equals(thatFilter)) {
                    return false;
                }
            }
        }
        return true;
    }

    private Object readResolve() throws InvalidObjectException {
        try {
            Predicate predicate = NOTHING;
            for (final Filter filter : filters) {
                predicate = predicate.add(filter);
            }
            return predicate;
        }
        catch (final Exception e) {
            throw (InvalidObjectException) new InvalidObjectException(
                    "Read invalid " + getClass().getSimpleName()).initCause(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public synchronized String toString() {
        return "Predicate [filters=" + filters + "]";
    }
}
