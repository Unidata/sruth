/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.Serializable;
import java.net.InetAddress;

/**
 * Information about a server
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class ServerInfo implements Serializable {
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

    private Object readResolve() {
        return new ServerInfo(inetAddress, ports);
    }
}
