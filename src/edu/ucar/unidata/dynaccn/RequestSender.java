/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Sends requests for data to the remote entity (server or client).
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class RequestSender {
    /**
     * The {@link ExecutorService} used by this class.
     */
    private static final ExecutorService executorService = Executors
                                                                 .newCachedThreadPool();
    /**
     * The associated object output stream.
     */
    private final ObjectOutputStream     objOutputStream;

    /**
     * Constructs from a socket.
     * 
     * @param socket
     *            The socket.
     * @throws IOException
     *             if an I/O error occurs while processing the socket.
     */
    private RequestSender(final OutputStream stream) throws IOException {
        objOutputStream = new ObjectOutputStream(stream);
    }

    /**
     * Creates a new instance and starts it.
     * 
     * @param objOutputStream
     *            The associated incoming request objOutputStream.
     * @return The future of the instance.
     * @throws IOException
     *             if an I/O error occurs while processing the objOutputStream.
     */
    static Future<Void> start(final OutputStream stream) throws IOException {
        final RequestSender instance = new RequestSender(stream);

        return executorService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {
                    instance.objOutputStream.writeObject(new Request());
                    // TODO
                }
                catch (final Exception e) {
                    try {
                        instance.objOutputStream.close();
                    }
                    catch (final IOException e1) {
                    }
                    throw e;
                }
                return null;
            }
        });
    }
}
