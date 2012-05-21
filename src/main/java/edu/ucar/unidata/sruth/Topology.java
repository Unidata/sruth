package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

/**
 * An n-to-m bidirectional mapping between servers and data-selection filters.
 * It is guaranteed that every server referenced by an instance of this class
 * will have at least one associated filter. A filter, however, may have no
 * associated servers.
 * <p>
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class Topology implements Serializable {
    /**
     * The serial version identifier.
     */
    private static final long                                  serialVersionUID = 1L;
    /**
     * The map from filters to servers.
     * 
     * @serial
     */
    @GuardedBy("this")
    private final NavigableMap<Filter, Set<InetSocketAddress>> serverSets       = new TreeMap<Filter, Set<InetSocketAddress>>();
    /**
     * The map from servers to filters.
     * 
     * @serial
     */
    @GuardedBy("this")
    private final Map<InetSocketAddress, Set<Filter>>          filterSets       = new HashMap<InetSocketAddress, Set<Filter>>();
    /**
     * A pseudo-random number generator.
     */
    @GuardedBy("this")
    private final transient Random                             random           = new Random();

    /**
     * Constructs from a set of data-selection filters.
     * 
     * @param filters
     *            The set of data-selection filters.
     */
    Topology(final Set<Filter> filters) {
        for (final Filter filter : filters) {
            final Set<InetSocketAddress> servers = newServerSet();
            serverSets.put(filter, servers);
        }
    }

    /**
     * Constructs an empty instance.
     */
    Topology() {
    }

    /**
     * Copy constructor.
     * 
     * @param that
     *            The other instance.
     */
    Topology(final Topology that) {
        Topology o1, o2;
        if (System.identityHashCode(this) < System.identityHashCode(that)) {
            o1 = this;
            o2 = that;
        }
        else {
            o1 = that;
            o2 = this;
        }
        synchronized (o1) {
            synchronized (o2) {
                /*
                 * Only the servers are iterated over because a filter might not
                 * have any associated servers and the {@link
                 * #add(InetSocketAddress, Set<Filter>)} method ensures a
                 * bi-directional mapping.
                 */
                for (final Map.Entry<InetSocketAddress, Set<Filter>> entry : that.filterSets
                        .entrySet()) {
                    add(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    /**
     * Returns the set of servers that can satisfy a file-selection filter, at
     * least. The set is not backed by this instance.
     * 
     * @param filter
     *            The given filter.
     * @return The set of associated servers. Might be empty.
     */
    synchronized Set<InetSocketAddress> getServers(final Filter filter) {
        final Set<InetSocketAddress> nodes = newServerSet();
        for (final Map.Entry<Filter, Set<InetSocketAddress>> entry : serverSets
                .entrySet()) {
            if (entry.getKey().includes(filter)) {
                nodes.addAll(entry.getValue());
            }
        }
        return nodes;
    }

    /**
     * Adds mappings between a filter and a server. Creates the mappings if they
     * don't already exist.
     * 
     * @param filter
     *            The file-selection filter.
     * @param server
     *            Information on the server.
     */
    synchronized void add(final Filter filter, final InetSocketAddress server) {
        Set<InetSocketAddress> servers = serverSets.get(filter);
        if (null == servers) {
            servers = newServerSet();
            serverSets.put(filter, servers);
        }
        servers.add(server);

        Set<Filter> filters = filterSets.get(server);
        if (filters == null) {
            filters = newFilterSet();
            filterSets.put(server, filters);
        }
        filters.add(filter);
    }

    /**
     * Adds bi-directional mappings between a filter and its set of servers.
     * Creates entries if they don't already exist.
     * 
     * @param filter
     *            The file-selection filter.
     * @param servers
     *            Information on the servers.
     */
    synchronized void add(final Filter filter,
            final Set<InetSocketAddress> servers) {
        Set<InetSocketAddress> serverInfos = serverSets.get(filter);
        if (null == serverInfos) {
            serverInfos = newServerSet();
            serverSets.put(filter, serverInfos);
        }
        serverInfos.addAll(servers);

        for (final InetSocketAddress server : servers) {
            Set<Filter> filters = filterSets.get(server);
            if (filters == null) {
                filters = newFilterSet();
                filterSets.put(server, filters);
            }
            filters.add(filter);
        }
    }

    /**
     * Adds bi-directional mappings between a sink-node's server and its set of
     * data-filters. Creates entries if they doesn't already exist.
     * 
     * @param server
     *            Address of the server.
     * @param filters
     *            The server's set of data-filters.
     */
    private synchronized void add(final InetSocketAddress server,
            final Set<Filter> filters) {
        Set<Filter> entryFilters = filterSets.get(server);
        if (entryFilters == null) {
            entryFilters = newFilterSet();
            filterSets.put(server, entryFilters);
        }
        entryFilters.addAll(filters);

        for (final Filter filter : filters) {
            Set<InetSocketAddress> entryServers = serverSets.get(filter);
            if (entryServers == null) {
                entryServers = newServerSet();
                serverSets.put(filter, entryServers);
            }
            entryServers.add(server);
        }
    }

    /**
     * Returns the set of servers in this instance. The returned set is not
     * backed by this instance.
     * 
     * @return The set of servers in this instance.
     */
    synchronized Set<InetSocketAddress> getServers() {
        final Set<InetSocketAddress> servers = newServerSet();
        servers.addAll(filterSets.keySet());
        return servers;
    }

    /**
     * Returns the subset of this instance that satisfies a given filter. The
     * returned instance is not backed-up by this instance. Each server in the
     * returned instance will be able to satisfy, at least, the given filter.
     * 
     * @param filter
     *            The filter to satisfy.
     * @return The subset of this instance that satisfies the given filter.
     */
    synchronized Topology subset(final Filter filter) {
        final Set<InetSocketAddress> servers = getServers(filter);
        final Topology subset = new Topology();
        for (final InetSocketAddress server : servers) {
            final Set<Filter> filters = filterSets.get(server);
            for (final Filter filt : filters) {
                if (filt.includes(filter)) {
                    subset.add(filt, server);
                }
            }
        }
        return subset;
    }

    /**
     * Removes a sink-node's server.
     * 
     * @param server
     *            The address of the sink-node's server.
     */
    synchronized void remove(final InetSocketAddress server) {
        final Set<Filter> filters = filterSets.remove(server);
        if (filters != null) {
            for (final Filter filter : filters) {
                final Set<InetSocketAddress> servers = serverSets.get(filter);
                if (servers != null) {
                    servers.remove(server);
                    if (servers.isEmpty()) {
                        serverSets.remove(filter);
                    }
                }
            }
        }
    }

    /**
     * Removes all the servers in a set of server addresses.
     * 
     * @param servers
     *            The set of server addresses to remove.
     */
    synchronized void remove(final Set<InetSocketAddress> servers) {
        // TODO: improve performance
        for (final InetSocketAddress server : servers) {
            remove(server);
        }
    }

    /**
     * Returns the best server to connect to for a given data-filter.
     * 
     * @param filter
     *            The specification of desired-data.
     * @return The best server to connect to or {@code null} if no such server
     *         exists.
     * @throws NullPointerException
     *             if {@code filter == null}.
     */
    synchronized InetSocketAddress getBestServer(final Filter filter) {
        /*
         * HEURISTIC: The best server to connect to is one that can just barely
         * satisfy the desired data.
         */
        final Set<InetSocketAddress> servers = getServers(filter);
        Filter targetFilter = Filter.EVERYTHING;
        final Set<InetSocketAddress> candidates = newServerSet();
        for (final InetSocketAddress server : servers) {
            final Set<Filter> filters = filterSets.get(server);
            if (filters != null) {
                for (final Filter f : filters) {
                    if (f.includes(filter) && targetFilter.includes(f)) {
                        if (!targetFilter.equals(f)) {
                            candidates.clear();
                            targetFilter = f;
                        }
                        candidates.add(server);
                        break;
                    }
                }
            }
        }

        final int size = candidates.size();
        if (size == 0) {
            return null;
        }

        /*
         * Pick a server at random from amongst the possible candidates.
         */
        int i = random.nextInt(size);
        final Iterator<InetSocketAddress> iter = candidates.iterator();
        while (i-- > 0) {
            iter.next();
        }
        return iter.next();
    }

    /**
     * Returns a new, empty set of servers.
     * 
     * @return a new, empty set of servers.
     */
    private Set<InetSocketAddress> newServerSet() {
        return new TreeSet<InetSocketAddress>(AddressComparator.INSTANCE);
    }

    /**
     * Returns a new, empty set of filters.
     * 
     * @return a new, empty set of filters.
     */
    private Set<Filter> newFilterSet() {
        return new TreeSet<Filter>();
    }

    /**
     * Returns the number of servers associated with a given filter.
     * 
     * @param filter
     *            The filter in question.
     * @return The number of servers associated with the given filter.
     * @throws NullPointerException
     *             if {@code filter} is unknown.
     */
    synchronized int getServerCount(final Filter filter) {
        return serverSets.get(filter).size();
    }

    /**
     * Returns the filter with the fewest number of associated servers or
     * {@code null} if this instance has no filters.
     * 
     * @return The filter with the fewest number of associated servers or
     *         {@code null}.
     */
    synchronized Filter getLeastFilter() {
        Filter filter = null;
        int n = Integer.MAX_VALUE;
        for (final Map.Entry<Filter, Set<InetSocketAddress>> entry : serverSets
                .entrySet()) {
            final int s = entry.getValue().size();
            if (s < n) {
                filter = entry.getKey();
                n = s;
            }
        }
        return filter;
    }

    /**
     * Returns the number of servers associated with the filter with the fewest
     * number of associated servers. If {@code 0} is returned, then
     * {@link #getLeastFilter()} will return {@code null}. This method is
     * equivalent to {@code getServerCount(getLeastFilter())} providing
     * {@code getLeastFilter()} returns non-null.
     * 
     * @return The number of servers associated with the filter with the fewest
     *         number of associated servers.
     * @see #getServerCount(Filter)
     * @see #getLeastFilter()
     */
    synchronized int getLeastFilterCount() {
        int n = Integer.MAX_VALUE;
        for (final Map.Entry<Filter, Set<InetSocketAddress>> entry : serverSets
                .entrySet()) {
            final int s = entry.getValue().size();
            if (s < n) {
                n = s;
            }
        }
        return n == Integer.MAX_VALUE
                ? 0
                : n;
    }

    /**
     * Clears this instance. After this call, this instance will contain no
     * filters and no servers.
     */
    synchronized void clear() {
        serverSets.clear();
        filterSets.clear();
    }

    /**
     * Indicates if this instance is empty (i.e., contains no filters and no
     * servers).
     * 
     * @return {@code true} if and only if this instance contains no filters and
     *         no servers.
     */
    synchronized boolean isEmpty() {
        return serverSets.isEmpty() && filterSets.isEmpty();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    synchronized public String toString() {
        return "Topology [serverSets=" + serverSets + ", filterSets="
                + filterSets + "]";
    }

    /**
     * Serializes this instance.
     * 
     * @throws IOException
     *             if an I/O error occurs.
     */
    synchronized private void writeObject(final ObjectOutputStream out)
            throws IOException {
        out.defaultWriteObject();
    }

    private Object readResolve() {
        final Topology instance = new Topology();
        for (final Map.Entry<Filter, Set<InetSocketAddress>> entry : serverSets
                .entrySet()) {
            instance.add(entry.getKey(), entry.getValue());
        }
        return instance;
    }
}