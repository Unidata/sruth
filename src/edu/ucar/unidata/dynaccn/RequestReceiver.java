/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved. See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Receives requests for data and acts upon them.
 * 
 * Instances are thread-safe.
 * 
 * @author Steven R. Emmerson
 */
final class RequestReceiver {
    /**
     * The {@link ExecutorService} used by this class.
     */
    private static final ExecutorService executorService = Executors
                                                                 .newCachedThreadPool();
    /**
     * The associated object input stream.
     */
    private final ObjectInputStream      objInputStream;

    /**
     * Constructs from a socket.
     * 
     * @param socket
     *            The socket.
     * @throws IOException
     *             if an I/O error occurs while processing the socket.
     */
    private RequestReceiver(final InputStream stream) throws IOException {
        objInputStream = new ObjectInputStream(stream);
    }

    /**
     * Creates a new instance and starts it.
     * 
     * @param stream
     *            The associated incoming request stream.
     * @return The future of the instance.
     * @throws IOException
     *             if an I/O error occurs while processing the stream.
     */
    static Future<Void> start(final InputStream stream) throws IOException {
        final RequestReceiver instance = new RequestReceiver(stream);

        return executorService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {
                    for (;;) {
                        instance.objInputStream.readObject();
                        // TODO
                    }
                }
                catch (final Exception e) {
                    try {
                        instance.objInputStream.close();
                    }
                    catch (final IOException e1) {
                    }
                    throw e;
                }
            }
        });
    }
}
