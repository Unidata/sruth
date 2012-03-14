/**
 * Copyright 2011 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.util.Iterator;

import net.jcip.annotations.Immutable;
import net.jcip.annotations.NotThreadSafe;

/**
 * A set of port numbers.
 * 
 * Instances are immutable
 * 
 * @author Steven R. Emmerson
 */
@Immutable
final class PortNumberSet implements Iterable<Integer> {
    /**
     * An iterator over the port numbers in the enclosing set.
     * 
     * Instances are thread-compatible but not thread-safe.
     * 
     * @author Steven R. Emmerson
     */
    @NotThreadSafe
    private final class PortNumberIterator implements Iterator<Integer> {
        /**
         * The next port number to be returned.
         */
        private int nextPort;

        /**
         * Constructs.
         */
        PortNumberIterator() {
            nextPort = minPort;
        }

        @Override
        public boolean hasNext() {
            return nextPort <= maxPort;
        }

        @Override
        public Integer next() {
            return nextPort++;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * The minimum possible port number.
     */
    static final int           MIN_PORT_NUMBER = 1;
    /**
     * The maximum possible port number ({@value} ).
     */
    static final int           MAX_PORT_NUMBER = (1 << 16) - 1;
    /**
     * The port number set that comprises the single element zero.
     */
    static final PortNumberSet EPHEMERAL       = new PortNumberSet(0, 0);
    /**
     * The minimum potential port number.
     */
    private final int          minPort;
    /**
     * The maximum potential port number.
     */
    private final int          maxPort;

    /**
     * Constructs from a contiguous range of numbers.
     * 
     * @param minPort
     *            The minimum port number.
     * @param maxPort
     *            The maximum port number.
     * @throws IllegalArgumentException
     *             if {@code minPort < 0 || maxPort < 0 || minPort > maxPort ||
     *             minPort > }{@link #MAX_PORT_NUMBER} {@code || maxPort > }
     *             {@link #MAX_PORT_NUMBER}
     */
    private PortNumberSet(final int minPort, final int maxPort) {
        if (minPort < 0 || maxPort < 0 || minPort > maxPort
                || minPort > MAX_PORT_NUMBER || maxPort > MAX_PORT_NUMBER) {
            throw new IllegalArgumentException("Invalid port range: minPort="
                    + minPort + ", maxPort=" + maxPort);
        }
        this.minPort = minPort;
        this.maxPort = maxPort;
    }

    /**
     * Returns an instance.
     * 
     * @param minPort
     *            The minimum port number. If zero, then a set comprising the
     *            single value zero will be returned.
     * @param maxPort
     *            The maximum port number.
     * @throws IllegalArgumentException
     *             if {@code minPort < 0 || maxPort < 0 || minPort > maxPort ||
     *             minPort > }{@link #MAX_PORT_NUMBER} {@code || maxPort > }
     *             {@link #MAX_PORT_NUMBER}
     */
    static PortNumberSet getInstance(final int minPort, final int maxPort) {
        return (minPort == 0)
                ? EPHEMERAL
                : new PortNumberSet(minPort, maxPort);
    }

    @Override
    public Iterator<Integer> iterator() {
        return new PortNumberIterator();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "PortNumberSet [minPort=" + minPort + ", maxPort=" + maxPort
                + "]";
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
        result = prime * result + maxPort;
        result = prime * result + minPort;
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
        final PortNumberSet other = (PortNumberSet) obj;
        if (maxPort != other.maxPort) {
            return false;
        }
        if (minPort != other.minPort) {
            return false;
        }
        return true;
    }
}
