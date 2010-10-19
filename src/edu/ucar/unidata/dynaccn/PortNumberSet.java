/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.util.Iterator;

import net.jcip.annotations.NotThreadSafe;

/**
 * A set of port numbers.
 * 
 * Instances are thread-compatible but not thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@NotThreadSafe
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
     * The maximum possible port number.
     */
    static final int           MAX_PORT_NUMBER = (1 << 16) - 1;
    /**
     * The port number set that comprises the single element zero.
     */
    static final PortNumberSet ZERO            = new PortNumberSet(0, 0);
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
     *             minPort > {@link #MAX_PORT_NUMBER} || maxPort >
     *             {@link #MAX_PORT_NUMBER}.
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
     *             minPort > {@link #MAX_PORT_NUMBER} || maxPort >
     *             {@link #MAX_PORT_NUMBER}.
     */
    static PortNumberSet getInstance(final int minPort, final int maxPort) {
        return (minPort == 0)
                ? ZERO
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
        return "PortNumberSet [maxPort=" + maxPort + ", minPort=" + minPort
                + "]";
    }
}
