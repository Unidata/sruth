/**
 * Copyright 2011 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

import net.jcip.annotations.ThreadSafe;

/**
 * A single Internet address plus a set of port numbers.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
@ThreadSafe
final class InetSocketAddressSet {
    /**
     * The Internet address.
     */
    // private final AtomicReference<InetAddress> inetAddress = new
    // AtomicReference<InetAddress>();
    private final InetAddress   inetAddress;

    /**
     * The set of port numbers.
     */
    // private final AtomicReference<PortNumberSet> portSet = new
    // AtomicReference<PortNumberSet>();
    private final PortNumberSet portSet;

    /**
     * Constructs from nothing. The Internet address will be the address of the
     * local host and the set of port numbers will be the ephemeral port number.
     * 
     * @throws UnknownHostException
     *             if the name of the local host couldn't be resolved into an
     *             address.
     */
    InetSocketAddressSet() throws UnknownHostException {
        this(null, null);
    }

    /**
     * Constructs from an Internet address. The set of port numbers will be the
     * ephemeral port number.
     * 
     * @param inetAddress
     *            The Internet address or {@code null}, in which case the
     *            wildcard address is used.
     * @throws UnknownHostException
     *             if the name of the local host couldn't be resolved into an
     *             address.
     */
    InetSocketAddressSet(final InetAddress inetAddress)
            throws UnknownHostException {
        this(inetAddress, null);
    }

    /**
     * Constructs from a set of port numbers. The Internet address will be the
     * address of the local host.
     * 
     * @param portSet
     *            The set of port numbers or {@code null}, in which case the
     *            ephemeral port number will be used.
     * @throws UnknownHostException
     *             if the name of the local host couldn't be resolved into an
     *             address.
     */
    InetSocketAddressSet(final PortNumberSet portSet)
            throws UnknownHostException {
        this(null, portSet);
    }

    /**
     * Constructs from an Internet address and a set of port numbers.
     * 
     * @param inetAddress
     *            The Internet address or {@code null}, in which case the
     *            address of the local host will be used.
     * @param portSet
     *            The set of port numbers or {@code null}, in which case the
     *            ephemeral port number will be used.
     * @throws UnknownHostException
     *             if the name of the local host couldn't be resolved into an
     *             address.
     */
    InetSocketAddressSet(InetAddress inetAddress, PortNumberSet portSet)
            throws UnknownHostException {
        if (inetAddress == null) {
            inetAddress = InetAddress.getLocalHost();
        }
        if (portSet == null) {
            portSet = PortNumberSet.EPHEMERAL;
        }
        this.inetAddress = inetAddress;
        this.portSet = portSet;
    }

    /**
     * Returns the Internet address
     * 
     * @return the Internet address or {@code null} if it's the wildcard
     *         address.
     */
    InetAddress getInetAddress() {
        return inetAddress;
    }

    /**
     * Returns the set of port numbers.
     * 
     * @return the set of port numbers.
     */
    PortNumberSet getPortNumberSet() {
        return portSet;
    }

    /**
     * Binds a server-socket to the first available Internet socket address of
     * this instance, if possible.
     * 
     * @param socket
     *            The server-socket to be bound.
     * @return {@code true} if and only if the socket was successfully bound to
     *         an Internet socket address of this instance.
     * @throws NullPointerException
     *             if {@code socket == null}.
     */
    boolean bind(final ServerSocket socket) {
        final PortNumberSet portSet = getPortNumberSet();
        final InetAddress inetAddress = getInetAddress();
        for (final int port : portSet) {
            try {
                InetSocketAddress inetSocketAddress;
                if (inetAddress == null) {
                    inetSocketAddress = new InetSocketAddress(port);
                }
                else {
                    inetSocketAddress = new InetSocketAddress(inetAddress, port);
                }
                socket.bind(inetSocketAddress);
                return true;
            }
            catch (final IOException ignored) {
            }
        }
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "InetSocketAddressSet [inetAddress=" + (inetAddress == null
                ? "ANY"
                : inetAddress) + ", portSet=" + portSet + "]";
    }
}
