/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.net.InetSocketAddress;
import java.util.Set;

import net.jcip.annotations.Immutable;
import edu.ucar.unidata.sruth.Connection.Message;

/**
 * Information on a node in the distribution network.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
@Immutable
final class NodeInfo implements Message {
    /**
     * Serial version number.
     */
    private static final long       serialVersionUID = 1L;
    /**
     * Specification of the desired data.
     */
    private final Predicate         predicate;
    /**
     * Information on the server.
     */
    private final InetSocketAddress inetSocketAddress;

    /**
     * Constructs from information on the server and a desired-data predicate.
     * 
     * @param inetSocketAddress
     *            Information on the server.
     * @param predicate
     *            Specification of the desired data.
     * @throws NullPointerException
     *             if {@code inetSocketAddress == null};
     * @throws NullPointerException
     *             if {@code predicate == null};
     */
    NodeInfo(final InetSocketAddress comparableInetSocketAddress,
            final Predicate predicate) {
        if (comparableInetSocketAddress == null) {
            throw new NullPointerException();
        }
        if (predicate == null) {
            throw new NullPointerException();
        }
        this.inetSocketAddress = comparableInetSocketAddress;
        this.predicate = predicate;
    }

    /**
     * @return the predicate
     */
    Predicate getPredicate() {
        return predicate;
    }

    /**
     * Returns the set of data-selection filters of this instance. The returned
     * set is not backed by this instance.
     * 
     * @return This instance's data-selection filters.
     */
    Set<Filter> getFilters() {
        return predicate.getFilters();
    }

    /**
     * @return the inetSocketAddress
     */
    InetSocketAddress getServerInfo() {
        return inetSocketAddress;
    }

    /**
     * Indicates if this instance is "better" than another instance. Such
     * instances are favored for servicing.
     * 
     * @param that
     *            The other instance.
     * @return {@code true} if and only if this instance is better than the
     *         other instance.
     * @throws NullPointerException
     *             if {@code that == null}.
     */
    boolean isBetterThan(final NodeInfo that) {
        return predicate.isMoreInclusiveThan(that.predicate);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "NodeInfo [predicate=" + predicate + ", inetSocketAddress="
                + inetSocketAddress + "]";
    }
}
