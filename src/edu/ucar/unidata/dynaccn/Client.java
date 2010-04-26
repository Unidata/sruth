/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights reserved.
 * See file LICENSE in the top-level source directory for licensing information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * Connects to a remote server and exchanges data.
 * 
 * @author Steven R. Emmerson
 */
final class Client implements Callable<Void> {
    /**
     * The connection to the remote server.
     */
    private final ClientConnection connection = new ClientConnection();
    /**
     * Pathname of the root of the file hierarchy.
     */
    private final File             dir;
    /**
     * Predicate for selecting locally-desired data.
     */
    private final Predicate        predicate;
    /**
     * Internet address of the server.
     */
    private final InetAddress      inetAddress;

    /**
     * Constructs from the Internet address of the remote server.
     * 
     * @param inetAddress
     *            The Internet address of the remote server.
     * @param dir
     *            Pathname of the root of the file hierarchy.
     * @param predicate
     *            The predicate for selecting locally-desired data.
     * @throws NullPointerException
     *             if {@code inetAddress == null || dir == null || predicate ==
     *             null}.
     */
    Client(final InetAddress inetAddress, final String dir,
            final Predicate predicate) throws IOException {
        if (null == predicate || null == inetAddress) {
            throw new NullPointerException();
        }

        this.dir = new File(dir);
        this.predicate = predicate;
        this.inetAddress = inetAddress;
    }

    /**
     * Executes this instance and waits upon one of the following conditions: 1)
     * all data that can be received has been received; 2) an error occurs; or
     * 3) the current thread is interrupted. In any case, any and all subtasks
     * will have been terminated upon return.
     * 
     * @throws IOException
     *             if an I/O error occurs while attempting to connect to the
     *             remote server.
     * @throws ExecutionException
     *             if this instance terminated due to an error.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    @Override
    public Void call() throws IOException, InterruptedException,
            ExecutionException {
        for (int i = 0; i < Connection.SOCKET_COUNT; i++) {
            connection.add(new Socket(inetAddress, Server.START_PORT + i));
        }
        try {
            System.out.println("Client: " + connection);
            return new Peer(connection, dir, predicate).call();
        }
        finally {
            connection.close();
        }
    }
}
