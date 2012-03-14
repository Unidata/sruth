/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import edu.ucar.unidata.sruth.Connection.Message;

/**
 * An Internet socket address that can be compared and transmitted between
 * nodes.
 * <p>
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class ComparableInetSocketAddress extends InetSocketAddress implements
        Comparable<ComparableInetSocketAddress>, Message {
    /**
     * The serial version ID.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Constructs from nothing. This constructor is equivalent to
     * {@link #ComparableInetSocketAddress(int) ComparableInetSocketAddress(0)}.
     */
    ComparableInetSocketAddress() {
        this(0);
    }

    /**
     * Constructs from a port number. This constructor is equivalent to
     * {@link #ComparableInetSocketAddress(InetAddress, int)
     * ComparableInetSocketAddress(null, port)}.
     * 
     * @param port
     *            The port number. If zero, then an ephemeral port will be
     *            assigned by the operating system.
     */
    ComparableInetSocketAddress(final int port) {
        this(null, port);
    }

    /**
     * Constructs from an IP address and a port number.
     * 
     * @param inetAddress
     *            The IP address. May be {@code null} to obtain the wildcard
     *            address.
     * @param port
     *            The port number. If {@code 0}, then an ephemeral port will be
     *            assigned by the operating-system during a
     *            {@link ServerSocket#bind()} operation.
     */
    ComparableInetSocketAddress(final InetAddress inetAddress, final int port) {
        super(inetAddress, port);
    }

    /**
     * Returns the IP address.
     * 
     * @return The IP address.
     */
    protected final InetAddress getInetAddress() {
        return getAddress();
    }

    /**
     * Returns a short name for this instance suitable for use as a filename.
     * 
     * @return a short name for this instance.
     */
    String getShortName() {
        return getHostString() + ":" + getPort();
    }

    @Override
    public final int compareTo(final ComparableInetSocketAddress that) {
        final byte[] thisBytes = this.getAddress().getAddress();
        final byte[] thatBytes = that.getAddress().getAddress();

        if (thisBytes.length < thatBytes.length) {
            return -1;
        }
        if (thisBytes.length > thatBytes.length) {
            return 1;
        }
        for (int i = 0; i < thisBytes.length; i++) {
            if (thisBytes[i] < thatBytes[i]) {
                return -1;
            }
            if (thisBytes[i] > thatBytes[i]) {
                return 1;
            }
        }
        final int thisPort = this.getPort();
        final int thatPort = that.getPort();
        return thisPort < thatPort
                ? -1
                : thisPort == thatPort
                        ? 0
                        : 1;
    }

    private Object readResolve() {
        return new ComparableInetSocketAddress(getAddress(), getPort());
    }
}
