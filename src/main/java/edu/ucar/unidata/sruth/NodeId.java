/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.Serializable;
import java.net.InetSocketAddress;

import net.jcip.annotations.Immutable;

/**
 * Uniquely identifies a node in the distribution network.
 * <p>
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
@Immutable
final class NodeId implements Serializable {
    /**
     * The serial version identifier.
     */
    private static final long       serialVersionUID = 1L;
    /**
     * The address of the node's server.
     */
    private final InetSocketAddress serverSocketAddress;

    /**
     * Constructs from the address of a node's server.
     * 
     * @param serverSocketAddress
     *            The address of a node's server.
     * @throws NullPointerException
     *             if {@code serverSocketAddress == null}.
     */
    NodeId(final InetSocketAddress serverSocketAddress) {
        if (serverSocketAddress == null) {
            throw new NullPointerException();
        }
        this.serverSocketAddress = serverSocketAddress;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((serverSocketAddress == null)
                ? 0
                : serverSocketAddress.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final NodeId other = (NodeId) obj;
        if (serverSocketAddress == null) {
            if (other.serverSocketAddress != null) {
                return false;
            }
        }
        else if (!serverSocketAddress.equals(other.serverSocketAddress)) {
            return false;
        }
        return true;
    }
}
