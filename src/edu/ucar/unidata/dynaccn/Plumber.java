/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import net.jcip.annotations.NotThreadSafe;

/**
 * A map whose keys are {@link ServerInfo}s and whose values are
 * {@link Predicate}s.
 * 
 * Instances are thread-compatible but not thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@NotThreadSafe
final class Plumber implements Serializable {
    /**
     * Serial version identifier.
     */
    private static final long                serialVersionUID = 1L;
    /**
     * The actual map.
     * 
     * @serial The actual map.
     */
    private final Map<ServerInfo, Predicate> map              = new HashMap<ServerInfo, Predicate>();

    /**
     * Adds a filter to the file-selection predicate associated with a server.
     * Creates an entry if it doesn't exist.
     * 
     * @param serverInfo
     *            Specification of the server.
     * @param filter
     *            The file-selection filter to be added to the server's
     *            predicate.
     */
    void add(final ServerInfo serverInfo, final Filter filter) {
        Predicate predicate = map.get(serverInfo);
        if (null == predicate) {
            predicate = new Predicate();
            map.put(serverInfo, predicate);
        }
        predicate.add(filter);
    }

    /**
     * Ensures a server-to-predicate mapping. Creates the entry if it doesn't
     * exist and replaces it otherwise. Returns the previous value or {@code
     * null} if no such entry existed.
     * 
     * @param serverInfo
     *            Information on the server.
     * @param predicate
     *            The file-selection predicate that the server can satisfy.
     * @return The previous value or {@code null}.
     */
    Predicate put(final ServerInfo serverInfo, final Predicate predicate) {
        return map.put(serverInfo, predicate);
    }

    /**
     * Connects a sink-node to the servers of this instance.
     * 
     * @param sinkNode
     *            The sink-node to be connected.
     */
    void connect(final SinkNode sinkNode) {
        for (final ServerInfo serverInfo : map.keySet()) {
            sinkNode.add(serverInfo);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Plumber [map=" + map + "]";
    }
}
