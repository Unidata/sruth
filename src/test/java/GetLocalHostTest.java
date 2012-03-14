import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Copyright 2011 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */

/**
 * Tests {@link java.net.InetAddress#getLocalHost()}.
 * 
 * @author Steven R. Emmerson
 */
final class GetLocalHostTest {
    public static void main(final String[] args) throws InterruptedException,
            UnknownHostException {
        final InetAddress localAddress = InetAddress.getLocalHost();
        System.out.println("localAddress=" + localAddress);
        System.out.println("localAddress.getCanonicalHostName()="
                + localAddress.getCanonicalHostName());
        System.out.println("localAddress.getHostAddress()="
                + localAddress.getHostAddress());
        System.out.println("localAddress.getHostName()="
                + localAddress.getHostName());
    }
}
