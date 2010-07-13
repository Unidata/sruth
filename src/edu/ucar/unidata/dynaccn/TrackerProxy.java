/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;

/**
 * A proxy for a tracker.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class TrackerProxy {
    /**
     * The address of the tracker's socket.
     */
    private final SocketAddress address;

    /**
     * Constructs from the Internet address of the tracker's socket.
     * 
     * @param address
     *            The address of the tracker's socket.
     * @throws NullPointerException
     *             if {@code address == null}.
     */
    TrackerProxy(final SocketAddress address) {
        if (null == address) {
            throw new NullPointerException();
        }
        this.address = address;
    }

    /**
     * Returns the server-to-predicate map obtained by querying the tracker.
     * 
     * This is a potentially lengthy operation.
     * 
     * @param inquisitor
     *            The query to send the tracker.
     * @return A map containing the servers to contact and the predicates to
     *         send them.
     * @throws IOException
     *             if an I/O exception occurs.
     * @throws ClassNotFoundException
     *             if the reply from the tracker is not understood.
     */
    Plumber getPlumber(final Inquisitor inquisitor) throws IOException,
            ClassNotFoundException {
        // TODO: institute a timeout mechanism
        final Socket socket = new Socket();
        socket.connect(address);
        final OutputStream outputStream = socket.getOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(outputStream);
        oos.writeObject(inquisitor);
        oos.flush();
        final InputStream inputStream = socket.getInputStream();
        final ObjectInputStream ois = new ObjectInputStream(inputStream);
        final Plumber map = (Plumber) ois.readObject();
        oos.close();
        ois.close();
        socket.close();
        return map;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "TrackerProxy [address=" + address + "]";
    }
}
