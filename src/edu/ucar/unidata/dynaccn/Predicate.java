/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.InvalidObjectException;
import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * A disjunction of filters for selecting data for a peer.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
class Predicate implements Serializable {
    /**
     * The serial version ID.
     */
    private static final long serialVersionUID = 1L;
    /**
     * The instance that's satisfied by everything.
     */
    static final Predicate    EVERYTHING       = new Predicate(new Filter[0]) {
                                                   /**
                                                    * The serial version ID.
                                                    */
                                                   private static final long serialVersionUID = 1L;

                                                   @Override
                                                   synchronized boolean satisfiedBy(
                                                           final FileInfo fileInfo) {
                                                       return true;
                                                   }

                                                   @Override
                                                   synchronized void removeIfPossible(
                                                           final FileInfo fileInfo) {
                                                   }

                                                   @Override
                                                   synchronized boolean satisfiedByNothing() {
                                                       return false;
                                                   }

                                                   @Override
                                                   public String toString() {
                                                       return "Predicate.EVERYTHING";
                                                   }

                                                   private Object readResolve() {
                                                       return EVERYTHING;
                                                   }
                                               };
    /**
     * The instance that's satisfied by nothing.
     */
    static final Predicate    NOTHING          = new Predicate(new Filter[0]) {
                                                   /**
                                                    * The serial version ID.
                                                    */
                                                   private static final long serialVersionUID = 1L;

                                                   @Override
                                                   synchronized boolean satisfiedBy(
                                                           final FileInfo fileInfo) {
                                                       return false;
                                                   }

                                                   @Override
                                                   synchronized void removeIfPossible(
                                                           final FileInfo fileInfo) {
                                                   }

                                                   @Override
                                                   synchronized boolean satisfiedByNothing() {
                                                       return true;
                                                   }

                                                   @Override
                                                   public String toString() {
                                                       return "Predicate.NOTHING";
                                                   }

                                                   private Object readResolve() {
                                                       return NOTHING;
                                                   }
                                               };
    /**
     * The filters.
     */
    private final Set<Filter> filters          = new ConcurrentSkipListSet<Filter>();

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
     * Indicates if a piece of data satisfies this predicate.
     * 
     * @param pieceSpec
     *            Information on the piece of data.
     * @return {@code true} if and only if the piece of data satisfies this
     *         predicate.
     * @throws NullPointerException
     *             if {@code pieceInfo == null}.
     */
    synchronized boolean satisfiedBy(final PieceSpec pieceSpec) {
        return satisfiedBy(pieceSpec);
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
    synchronized boolean satisfiedByNothing() {
        return filters.isEmpty();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{filters=(" + filters.size()
                + " filters)}";
    }

    private Object readResolve() throws InvalidObjectException {
        try {
            return new Predicate(filters.toArray(new Filter[filters.size()]));
        }
        catch (final Exception e) {
            throw (InvalidObjectException) new InvalidObjectException(
                    "Read invalid " + getClass().getSimpleName()).initCause(e);
        }
    }
}
