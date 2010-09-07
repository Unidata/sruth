/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.Arrays;

/**
 * Information about a server
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class ServerInfo implements Serializable, Comparable<ServerInfo> {
    /**
     * The serial version ID.
     */
    private static final long serialVersionUID = 1L;
    /**
     * The IP address of the host executing the server
     */
    private final InetAddress inetAddress;
    /**
     * The port numbers on which the server is listening.
     */
    private final int[]       ports;

    /**
     * Constructs from the IP address and the port numbers.
     * 
     * @param inetAddress
     *            The IP address of the host on which the server is running.
     * @param ports
     *            The port numbers on which the server is listening.
     * @throws NullPointerException
     *             if {@code inetAddress == null || ports == null}.
     */
    ServerInfo(final InetAddress inetAddress, final int[] ports) {
        if (null == inetAddress || null == ports) {
            throw new NullPointerException();
        }
        this.inetAddress = inetAddress;
        this.ports = ports;
    }

    /**
     * Returns the IP address of the host on which the server is running.
     * 
     * @return The IP address of the host on which the server is running.
     */
    InetAddress getInetAddress() {
        return inetAddress;
    }

    /**
     * Returns the port numbers on which the server is listening.
     * 
     * @return The port numbers on which the server is listening.
     */
    int[] getPorts() {
        return ports;
    }

    @Override
    public int compareTo(final ServerInfo that) {
        final byte[] thisBytes = inetAddress.getAddress();
        final byte[] thatBytes = that.inetAddress.getAddress();
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
        for (int i = 0; i < ports.length; i++) {
            if (ports[i] < that.ports[i]) {
                return -1;
            }
            if (ports[i] > that.ports[i]) {
                return 1;
            }
        }
        return 0;
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
        result = prime * result + ((inetAddress == null)
                ? 0
                : inetAddress.hashCode());
        result = prime * result + Arrays.hashCode(ports);
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
        final ServerInfo other = (ServerInfo) obj;
        if (inetAddress == null) {
            if (other.inetAddress != null) {
                return false;
            }
        }
        return compareTo(other) == 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "ServerInfo [inetAddress=" + inetAddress + ", ports="
                + Arrays.toString(ports) + "]";
    }

    private Object readResolve() {
        return new ServerInfo(inetAddress, ports);
    }
}
