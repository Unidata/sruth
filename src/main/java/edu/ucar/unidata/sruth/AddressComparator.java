/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Comparator;

/**
 * Compares Internet socket addresses.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class AddressComparator implements Comparator<InetSocketAddress>,
        Serializable {
    /**
     * The serial version identifier.
     */
    private static final long      serialVersionUID = 1L;
    /**
     * The singleton instance of this class.
     */
    static final AddressComparator INSTANCE         = new AddressComparator();

    /**
     * Constructs from nothing.
     */
    private AddressComparator() {
    }

    /**
     * Compares two Internet socket addresses.
     * 
     * @param a1
     *            The first address
     * @param a2
     *            The second address
     * @return a value less than, equal to, or greater than zero as the first
     *         address is considered less than, equal to, or greater than the
     *         second address.
     */
    public int compare(final InetSocketAddress a1, final InetSocketAddress a2) {
        final byte[] a1Bytes = a1.getAddress().getAddress();
        final byte[] a2Bytes = a2.getAddress().getAddress();

        if (a1Bytes.length < a2Bytes.length) {
            return -1;
        }
        if (a1Bytes.length > a2Bytes.length) {
            return 1;
        }
        for (int i = 0; i < a1Bytes.length; i++) {
            if (a1Bytes[i] < a2Bytes[i]) {
                return -1;
            }
            if (a1Bytes[i] > a2Bytes[i]) {
                return 1;
            }
        }
        final int a1Port = a1.getPort();
        final int a2Port = a2.getPort();
        return a1Port < a2Port
                ? -1
                : a1Port == a2Port
                        ? 0
                        : 1;
    }

    private Object readResolve() {
        return INSTANCE;
    }
}
